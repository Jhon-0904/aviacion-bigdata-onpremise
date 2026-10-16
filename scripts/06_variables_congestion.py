from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    when,
    hour,
    lit,
    concat_ws,
    date_format,
    unix_timestamp,
    sum as spark_sum
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("06_VariablesCongestion")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/staging/bts_prop"
RUTA_SALIDA = "hdfs:///proyecto_latam/staging/bts_congestion"

print("=" * 80)
print("MODULO 06 - VARIABLES DE CONGESTION AEROPORTUARIA")
print("=" * 80)

# ------------------------------------------------------------
# 1. Lectura del dataset con propagación
# ------------------------------------------------------------

vuelos = spark.read.parquet(RUTA_ENTRADA)

print("Registros de entrada:", vuelos.count())

# ------------------------------------------------------------
# 2. Variables auxiliares de tiempo
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "scheduled_dep_epoch",
        unix_timestamp(col("scheduled_dep_utc"))
    )
    .withColumn(
        "scheduled_arr_epoch",
        unix_timestamp(col("scheduled_arr_utc"))
    )
    .withColumn(
        "airport_hour",
        hour(col("scheduled_dep_local"))
    )
    .withColumn(
        "airport_hour_key",
        concat_ws(
            "_",
            col("Origin"),
            date_format(
                col("scheduled_dep_local"),
                "yyyy-MM-dd-HH"
            )
        )
    )
)

# ------------------------------------------------------------
# 3. Salidas programadas durante la hora anterior
# ------------------------------------------------------------

ventana_1h = (
    Window
    .partitionBy("Origin")
    .orderBy(col("scheduled_dep_epoch"))
    .rangeBetween(-3600, -1)
)

ventana_2h = (
    Window
    .partitionBy("Origin")
    .orderBy(col("scheduled_dep_epoch"))
    .rangeBetween(-7200, -1)
)

vuelos = (
    vuelos
    .withColumn(
        "airport_departures_previous_1h",
        count("*").over(ventana_1h)
    )
    .withColumn(
        "airport_departures_previous_2h",
        count("*").over(ventana_2h)
    )
)

# ------------------------------------------------------------
# 4. Vuelos activos en la misma hora programada
# ------------------------------------------------------------

actividad_hora = (
    vuelos
    .groupBy("Origin", "scheduled_year", "scheduled_month", "airport_hour")
    .agg(
        count("*").alias("airport_active_flights_same_hour")
    )
)

vuelos = vuelos.join(
    actividad_hora,
    [
        "Origin",
        "scheduled_year",
        "scheduled_month",
        "airport_hour"
    ],
    "left"
)

# ------------------------------------------------------------
# 5. Historial previo del aeropuerto
# ------------------------------------------------------------

ventana_aeropuerto = (
    Window
    .partitionBy("Origin")
    .orderBy(col("scheduled_dep_epoch"))
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)

vuelos = (
    vuelos
    .withColumn(
        "airport_previous_delay_rate",
        avg(
            col("delay_target").cast("double")
        ).over(ventana_aeropuerto)
    )
    .withColumn(
        "airport_previous_avg_delay",
        avg(
            col("delay_minutes_target")
        ).over(ventana_aeropuerto)
    )
)

# ------------------------------------------------------------
# 6. Historial del aeropuerto por hora
# ------------------------------------------------------------

ventana_aeropuerto_hora = (
    Window
    .partitionBy("Origin", "airport_hour")
    .orderBy(col("scheduled_dep_epoch"))
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)

vuelos = vuelos.withColumn(
    "airport_hour_delay_rate",
    avg(
        col("delay_target").cast("double")
    ).over(ventana_aeropuerto_hora)
)

# ------------------------------------------------------------
# 7. Valores iniciales controlados
# ------------------------------------------------------------

# Estas dos variables se completarán en un agregado separado
# utilizando los registros BTS cancelados y desviados.
vuelos = (
    vuelos
    .withColumn(
        "airport_previous_cancelled_count",
        lit(None).cast("long")
    )
    .withColumn(
        "airport_previous_diverted_count",
        lit(None).cast("long")
    )
)

# ------------------------------------------------------------
# 8. Verificación
# ------------------------------------------------------------

print("Registros finales:", vuelos.count())
print("Columnas finales:", len(vuelos.columns))

variables = [
    "airport_departures_previous_1h",
    "airport_departures_previous_2h",
    "airport_active_flights_same_hour",
    "airport_previous_delay_rate",
    "airport_previous_avg_delay",
    "airport_hour_delay_rate",
    "airport_previous_cancelled_count",
    "airport_previous_diverted_count"
]

print("\nVARIABLES DE CONGESTION")

for variable in variables:
    if variable in vuelos.columns:
        print(variable + ": OK")
    else:
        print(variable + ": FALTA")

# ------------------------------------------------------------
# 9. Escritura
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
