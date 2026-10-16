from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    lpad,
    concat,
    lit,
    to_timestamp,
    to_utc_timestamp,
    when,
    year,
    month,
    dayofweek,
    hour,
    minute,
    expr
)

spark = (
    SparkSession.builder
    .appName("04_PrepararBTS")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_BTS = "hdfs:///proyecto_latam/raw/bts"
RUTA_MAPEO = "hdfs:///proyecto_latam/reference/mapeo_aeropuerto_estacion.csv"
RUTA_SALIDA = "hdfs:///proyecto_latam/staging/bts_preparado"

print("=" * 80)
print("MODULO 04 - PREPARACION DISTRIBUIDA DE BTS")
print("=" * 80)

# ------------------------------------------------------------
# 1. Lectura de los archivos BTS desde HDFS
# ------------------------------------------------------------

bts = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(RUTA_BTS)
)

print("Registros BTS iniciales:", bts.count())

# ------------------------------------------------------------
# 2. Selección de las columnas necesarias
# ------------------------------------------------------------

columnas_bts = [
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "CRSArrTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "Cancelled",
    "Diverted",
    "Distance",
    "DistanceGroup"
]

vuelos = bts.select(*columnas_bts)

# ------------------------------------------------------------
# 3. Limpieza y normalización de texto
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn("Origin", trim(col("Origin")))
    .withColumn("Dest", trim(col("Dest")))
    .withColumn(
        "Reporting_Airline",
        trim(col("Reporting_Airline"))
    )
    .withColumn(
        "Tail_Number",
        trim(col("Tail_Number"))
    )
    .withColumn(
        "FlightDate",
        trim(col("FlightDate"))
    )
)

# ------------------------------------------------------------
# 4. Exclusión de vuelos no utilizables
# ------------------------------------------------------------

vuelos = (
    vuelos
    .filter(col("Origin").isNotNull())
    .filter(col("Dest").isNotNull())
    .filter(col("FlightDate").isNotNull())
    .filter(col("CRSDepTime").isNotNull())
    .filter(col("DepDel15").isNotNull())
    .filter(
        (col("Cancelled") == 0) |
        col("Cancelled").isNull()
    )
    .filter(
        (col("Diverted") == 0) |
        col("Diverted").isNull()
    )
)

print("Vuelos elegibles después de la limpieza:", vuelos.count())

# ------------------------------------------------------------
# 5. Conversión de CRSDepTime a hora local válida
# ------------------------------------------------------------

hora_salida = lpad(
    regexp_replace(
        col("CRSDepTime").cast("string"),
        "\\.0$",
        ""
    ),
    4,
    "0"
)

vuelos = vuelos.withColumn(
    "scheduled_dep_local",
    to_timestamp(
        concat(
            col("FlightDate"),
            lit(" "),
            hora_salida.substr(1, 2),
            lit(":"),
            hora_salida.substr(3, 2),
            lit(":00")
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# ------------------------------------------------------------
# 6. Lectura del mapeo de aeropuertos y estaciones NOAA
# ------------------------------------------------------------

mapeo = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(RUTA_MAPEO)
)

mapeo = mapeo.select(
    col("origin").alias("map_origin"),
    col("station_id").alias("origin_station"),
    col("time_zone").alias("origin_time_zone"),
    col("station_name").alias("origin_station_name")
)

# ------------------------------------------------------------
# 7. Unión BTS con estación NOAA y zona horaria
# ------------------------------------------------------------

vuelos = (
    vuelos
    .join(
        mapeo,
        vuelos["Origin"] == mapeo["map_origin"],
        "inner"
    )
    .drop("map_origin")
)

print("Vuelos con estación NOAA asignada:", vuelos.count())

# ------------------------------------------------------------
# 8. Conversión de la hora local de salida a UTC
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "scheduled_dep_utc",
        when(
            col("origin_time_zone") == "America/Denver",
            to_utc_timestamp(
                col("scheduled_dep_local"),
                "America/Denver"
            )
        )
        .when(
            col("origin_time_zone") == "America/Chicago",
            to_utc_timestamp(
                col("scheduled_dep_local"),
                "America/Chicago"
            )
        )
        .when(
            col("origin_time_zone") == "America/New_York",
            to_utc_timestamp(
                col("scheduled_dep_local"),
                "America/New_York"
            )
        )
        .when(
            col("origin_time_zone") == "America/Los_Angeles",
            to_utc_timestamp(
                col("scheduled_dep_local"),
                "America/Los_Angeles"
            )
        )
        .when(
            col("origin_time_zone") == "America/Phoenix",
            to_utc_timestamp(
                col("scheduled_dep_local"),
                "America/Phoenix"
            )
        )
    )
)

# ------------------------------------------------------------
# 9. Creación del momento de alerta T-2 horas
# ------------------------------------------------------------

vuelos = vuelos.withColumn(
    "alert_time_utc",
    col("scheduled_dep_utc") -
    expr("INTERVAL 2 HOURS")
)

# ------------------------------------------------------------
# 10. Variables de calendario y objetivo
# ------------------------------------------------------------

vuelos = (
    vuelos
    .withColumn(
        "scheduled_year",
        year(col("scheduled_dep_local"))
    )
    .withColumn(
        "scheduled_month",
        month(col("scheduled_dep_local"))
    )
    .withColumn(
        "scheduled_day_of_week",
        dayofweek(col("scheduled_dep_local"))
    )
    .withColumn(
        "scheduled_hour",
        hour(col("scheduled_dep_local"))
    )
    .withColumn(
        "scheduled_minute",
        minute(col("scheduled_dep_local"))
    )
    .withColumn(
        "is_weekend",
        when(
            col("scheduled_day_of_week").isin([1, 7]),
            1
        ).otherwise(0)
    )
    .withColumn(
        "delay_target",
        col("DepDel15").cast("int")
    )
    .withColumn(
        "delay_minutes_target",
        col("DepDelayMinutes").cast("double")
    )
)

# ------------------------------------------------------------
# 11. Eliminación de duplicados
# ------------------------------------------------------------

vuelos = vuelos.dropDuplicates([
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime"
])

# ------------------------------------------------------------
# 12. Verificación de columnas generadas
# ------------------------------------------------------------

print("Columnas generadas:", len(vuelos.columns))
print("Registros finales:", vuelos.count())

print("\nVERIFICACION DE VARIABLES")

variables = [
    "scheduled_dep_local",
    "scheduled_dep_utc",
    "alert_time_utc",
    "scheduled_year",
    "scheduled_month",
    "scheduled_day_of_week",
    "scheduled_hour",
    "scheduled_minute",
    "is_weekend",
    "delay_target",
    "delay_minutes_target",
    "origin_station",
    "origin_time_zone"
]

for variable in variables:
    if variable in vuelos.columns:
        print(variable + ": OK")
    else:
        print(variable + ": FALTA")

# ------------------------------------------------------------
# 13. Escritura en la zona staging
# ------------------------------------------------------------

(
    vuelos
    .repartition(
        "scheduled_year",
        "scheduled_month",
        "Origin"
    )
    .write
    .mode("overwrite")
    .partitionBy(
        "scheduled_year",
        "scheduled_month",
        "Origin"
    )
    .parquet(RUTA_SALIDA)
)

print("\nZONA STAGING GENERADA:")
print(RUTA_SALIDA)

spark.stop()
