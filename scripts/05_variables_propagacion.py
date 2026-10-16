from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lpad,
    regexp_replace,
    concat,
    lit,
    to_timestamp,
    to_utc_timestamp,
    when,
    expr,
    lag,
    count,
    avg,
    max as spark_max,
    sum as spark_sum
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("05_VariablesPropagacion")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/staging/bts_preparado"
RUTA_MAPEO = "hdfs:///proyecto_latam/reference/mapeo_aeropuerto_estacion.csv"
RUTA_SALIDA = "hdfs:///proyecto_latam/staging/bts_prop"

print("=" * 80)
print("MODULO 05 - VARIABLES DE PROPAGACION DE AERONAVES")
print("=" * 80)

# ------------------------------------------------------------
# 1. Lectura del dataset preparado
# ------------------------------------------------------------

vuelos = (
    spark.read
    .parquet(RUTA_ENTRADA)
)

print("Registros de entrada:", vuelos.count())

# ------------------------------------------------------------
# 2. Lectura del mapeo de zonas horarias de destino
# ------------------------------------------------------------

mapeo = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(RUTA_MAPEO)
)

mapeo_destino = mapeo.select(
    col("origin").alias("dest_code"),
    col("time_zone").alias("dest_time_zone")
)

# ------------------------------------------------------------
# 3. Incorporar zona horaria del destino
# ------------------------------------------------------------

vuelos = (
    vuelos
    .join(
        mapeo_destino,
        vuelos["Dest"] == mapeo_destino["dest_code"],
        "left"
    )
    .drop("dest_code")
)

# ------------------------------------------------------------
# 4. Crear hora programada de llegada local
# ------------------------------------------------------------

hora_llegada = lpad(
    regexp_replace(
        col("CRSArrTime").cast("string"),
        "\\.0$",
        ""
    ),
    4,
    "0"
)

vuelos = vuelos.withColumn(
    "scheduled_arr_local",
    when(
        hora_llegada.substr(1, 2) == "24",
        to_timestamp(
            concat(
                col("FlightDate"),
                lit(" 00:00:00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        ) + expr("INTERVAL 1 DAY")
    ).otherwise(
        to_timestamp(
            concat(
                col("FlightDate"),
                lit(" "),
                hora_llegada.substr(1, 2),
                lit(":"),
                hora_llegada.substr(3, 2),
                lit(":00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)
# ------------------------------------------------------------
# 5. Corregir llegadas que cruzan la medianoche
# ------------------------------------------------------------

vuelos = vuelos.withColumn(
    "scheduled_arr_local",
    when(
        col("scheduled_arr_local") <
        col("scheduled_dep_local"),
        col("scheduled_arr_local") +
        expr("INTERVAL 1 DAY")
    ).otherwise(col("scheduled_arr_local"))
)
# ------------------------------------------------------------
# 6. Convertir llegada programada a UTC
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "scheduled_arr_utc",
        when(
            col("dest_time_zone") == "America/Denver",
            to_utc_timestamp(
                col("scheduled_arr_local"),
                "America/Denver"
            )
        )
        .when(
            col("dest_time_zone") == "America/Chicago",
            to_utc_timestamp(
                col("scheduled_arr_local"),
                "America/Chicago"
            )
        )
        .when(
            col("dest_time_zone") == "America/New_York",
            to_utc_timestamp(
                col("scheduled_arr_local"),
                "America/New_York"
            )
        )
        .when(
            col("dest_time_zone") == "America/Los_Angeles",
            to_utc_timestamp(
                col("scheduled_arr_local"),
                "America/Los_Angeles"
            )
        )
        .when(
            col("dest_time_zone") == "America/Phoenix",
            to_utc_timestamp(
                col("scheduled_arr_local"),
                "America/Phoenix"
            )
        )
    )
)

# ------------------------------------------------------------
# 7. Orden cronológico por matrícula
# ------------------------------------------------------------

ventana_avion = (
    Window
    .partitionBy("Tail_Number")
    .orderBy(
        col("scheduled_dep_utc"),
        col("Flight_Number_Reporting_Airline")
    )
)

vuelos = (
    vuelos
    .withColumn(
        "previous_flight_delay_raw",
        lag("DepDelayMinutes").over(ventana_avion)
    )
    .withColumn(
        "previous_flight_scheduled_arrival",
        lag("scheduled_arr_utc").over(ventana_avion)
    )
    .withColumn(
        "previous_flight_origin",
        lag("Origin").over(ventana_avion)
    )
    .withColumn(
        "previous_flight_dest",
        lag("Dest").over(ventana_avion)
    )
)

# ------------------------------------------------------------
# 8. Validación contra fuga de información
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "previous_flight_delay",
        when(
            col("previous_flight_scheduled_arrival")
            <= col("alert_time_utc"),
            col("previous_flight_delay_raw")
        )
    )
    .withColumn(
        "turnaround_minutes",
        when(
            col("previous_flight_scheduled_arrival")
            <= col("scheduled_dep_utc"),
            (
                col("scheduled_dep_utc").cast("long")
                - col("previous_flight_scheduled_arrival").cast("long")
            ) / 60.0
        )
    )
)

# ------------------------------------------------------------
# 9. Variables históricas de la aeronave
# ------------------------------------------------------------

ventana_historial_avion = (
    Window
    .partitionBy("Tail_Number")
    .orderBy(
        col("scheduled_dep_utc"),
        col("Flight_Number_Reporting_Airline")
    )
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)

vuelos = (
    vuelos
    .withColumn(
        "aircraft_previous_flights",
        count("*").over(ventana_historial_avion)
    )
    .withColumn(
        "aircraft_recent_avg_delay",
        avg("previous_flight_delay").over(
            ventana_historial_avion
        )
    )
    .withColumn(
        "aircraft_recent_max_delay",
        spark_max("previous_flight_delay").over(
            ventana_historial_avion
        )
    )
    .withColumn(
        "aircraft_previous_delay_rate",
        avg(
            when(
                col("previous_flight_delay") >= 15,
                1.0
            ).otherwise(0.0)
        ).over(ventana_historial_avion)
    )
    .withColumn(
        "aircraft_previous_delayed_count",
        spark_sum(
            when(
                col("previous_flight_delay") >= 15,
                1
            ).otherwise(0)
        ).over(ventana_historial_avion)
    )
)

# ------------------------------------------------------------
# 10. Control de rangos
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "turnaround_minutes",
        when(
            (col("turnaround_minutes") >= 0) &
            (col("turnaround_minutes") <= 1440),
            col("turnaround_minutes")
        )
    )
)

# ------------------------------------------------------------
# 11. Verificación de variables
# ------------------------------------------------------------

print("Registros finales:", vuelos.count())
print("Columnas finales:", len(vuelos.columns))

print("\nVARIABLES DE PROPAGACION")

variables = [
    "scheduled_arr_utc",
    "previous_flight_delay",
    "previous_flight_scheduled_arrival",
    "previous_flight_origin",
    "previous_flight_dest",
    "turnaround_minutes",
    "aircraft_previous_flights",
    "aircraft_recent_avg_delay",
    "aircraft_recent_max_delay",
    "aircraft_previous_delay_rate",
    "aircraft_previous_delayed_count"
]

for variable in variables:
    if variable in vuelos.columns:
        print(variable + ": OK")
    else:
        print(variable + ": FALTA")

print("\nCOMPLETITUD DE VARIABLES")

vuelos.select(
    "previous_flight_delay",
    "turnaround_minutes",
    "aircraft_recent_avg_delay",
    "aircraft_recent_max_delay",
    "aircraft_previous_delay_rate",
    "aircraft_previous_delayed_count"
).summary("count").show(truncate=False)


# ------------------------------------------------------------
# 12. Escritura de salida
# ------------------------------------------------------------

(
    vuelos
    .write
    .mode("overwrite")
    .partitionBy(
        "scheduled_year",
        "scheduled_month",
        "Origin"
    )
    .parquet(RUTA_SALIDA)
)

print("\nZONA GENERADA:")
print(RUTA_SALIDA)

spark.stop()
