from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    expr,
    lit,
    row_number,
    unix_timestamp,
    when
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("08_IntegrarBTSNOAA")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "96")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_BTS = "hdfs:///proyecto_latam/staging/bts_operativo"
RUTA_NOAA = "hdfs:///proyecto_latam/raw/noaa"
RUTA_SALIDA = "hdfs:///proyecto_latam/processed/integrado"

print("=" * 80)
print("MODULOS 08–09 - NOAA E INTEGRACION TEMPORAL")
print("=" * 80)

# ------------------------------------------------------------
# 1. Lectura BTS operativo
# ------------------------------------------------------------

vuelos = (
    spark.read
    .parquet(RUTA_BTS)
    .withColumn(
        "flight_id",
        expr("monotonically_increasing_id()")
    )
    .alias("v")
)

print("Vuelos de entrada:", vuelos.count())

# ------------------------------------------------------------
# 2. Lectura NOAA GHCNh
# ------------------------------------------------------------

clima_raw = (
    spark.read
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(RUTA_NOAA)
)

print("Observaciones NOAA iniciales:", clima_raw.count())

# ------------------------------------------------------------
# 3. Normalización NOAA
# ------------------------------------------------------------

clima = (
    clima_raw
    .select(
        trim(col("STATION")).alias("station_id"),
        col("DATE").alias("weather_date"),
        col("temperature").alias("temperature_raw"),
        col("dew_point_temperature").alias(
            "dew_point_temperature_raw"
        ),
        col("relative_humidity").alias(
            "relative_humidity_raw"
        ),
        col("wind_direction").alias(
            "wind_direction_raw"
        ),
        col("wind_speed").alias("wind_speed_raw"),
        col("wind_gust").alias("wind_gust_raw"),
        col("visibility").alias("visibility_raw"),
        col("station_level_pressure").alias(
            "station_level_pressure_raw"
        ),
        col("sea_level_pressure").alias(
            "sea_level_pressure_raw"
        ),
        col("precipitation").alias(
            "precipitation_raw"
        )
    )
    .withColumn(
        "weather_time_utc",
        to_timestamp(
            col("weather_date"),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )
)

# Conversión segura a DOUBLE.
# Los valores no convertibles se transforman en NULL.
clima = (
    clima
    .withColumn(
        "temperature",
        expr("try_cast(temperature_raw AS DOUBLE)")
    )
    .withColumn(
        "dew_point_temperature",
        expr(
            "try_cast(dew_point_temperature_raw AS DOUBLE)"
        )
    )
    .withColumn(
        "relative_humidity",
        expr(
            "try_cast(relative_humidity_raw AS DOUBLE)"
        )
    )
    .withColumn(
        "wind_direction",
        expr("try_cast(wind_direction_raw AS DOUBLE)")
    )
    .withColumn(
        "wind_speed",
        expr("try_cast(wind_speed_raw AS DOUBLE)")
    )
    .withColumn(
        "wind_gust",
        expr("try_cast(wind_gust_raw AS DOUBLE)")
    )
    .withColumn(
        "visibility",
        expr("try_cast(visibility_raw AS DOUBLE)")
    )
    .withColumn(
        "station_level_pressure",
        expr(
            "try_cast(station_level_pressure_raw AS DOUBLE)"
        )
    )
    .withColumn(
        "sea_level_pressure",
        expr(
            "try_cast(sea_level_pressure_raw AS DOUBLE)"
        )
    )
    .withColumn(
        "precipitation",
        expr(
            "try_cast(precipitation_raw AS DOUBLE)"
        )
    )
    .select(
        "station_id",
        "weather_time_utc",
        "temperature",
        "dew_point_temperature",
        "relative_humidity",
        "wind_direction",
        "wind_speed",
        "wind_gust",
        "visibility",
        "station_level_pressure",
        "sea_level_pressure",
        "precipitation"
    )
    .filter(col("weather_time_utc").isNotNull())
    .alias("c")
)

print("Observaciones NOAA normalizadas:", clima.count())

# ------------------------------------------------------------
# 4. Unión temporal BTS–NOAA
# ------------------------------------------------------------

condicion = (
    (col("v.origin_station") == col("c.station_id")) &
    (col("c.weather_time_utc") <= col("v.alert_time_utc")) &
    (
        col("c.weather_time_utc") >=
        col("v.alert_time_utc") -
        expr("INTERVAL 6 HOURS")
    )
)

cruce = vuelos.join(
    clima,
    condicion,
    "left"
)

# ------------------------------------------------------------
# 5. Selección de la última observación anterior a T−2h
# ------------------------------------------------------------

ventana_clima = (
    Window
    .partitionBy("flight_id")
    .orderBy(col("c.weather_time_utc").desc())
)

integrado = (
    cruce
    .withColumn(
        "weather_rank",
        row_number().over(ventana_clima)
    )
    .filter(col("weather_rank") == 1)
    .drop("weather_rank")
)

# ------------------------------------------------------------
# 6. Indicadores meteorológicos
# ------------------------------------------------------------

integrado = (
    integrado
    .withColumn(
        "has_weather_match",
        when(
            col("weather_time_utc").isNotNull(),
            1
        ).otherwise(0)
    )
    .withColumn(
        "weather_age_minutes",
        when(
            col("weather_time_utc").isNotNull(),
            (
                unix_timestamp(col("alert_time_utc")) -
                unix_timestamp(col("weather_time_utc"))
            ) / 60.0
        )
    )
    .withColumn(
        "has_missing_weather",
        when(
            col("weather_time_utc").isNull(),
            1
        ).otherwise(0)
    )
    .drop("flight_id")
)

# ------------------------------------------------------------
# 7. Verificación
# ------------------------------------------------------------

print("Registros integrados:", integrado.count())
print("Columnas integradas:", len(integrado.columns))

print("\nVARIABLES METEOROLOGICAS")

variables = [
    "temperature",
    "dew_point_temperature",
    "relative_humidity",
    "wind_direction",
    "wind_speed",
    "wind_gust",
    "visibility",
    "station_level_pressure",
    "sea_level_pressure",
    "precipitation",
    "weather_time_utc",
    "weather_age_minutes",
    "has_weather_match",
    "has_missing_weather"
]

for variable in variables:
    if variable in integrado.columns:
        print(variable + ": OK")
    else:
        print(variable + ": FALTA")

print("\nConteo de correspondencias meteorológicas:")
integrado.groupBy("has_weather_match").count().show()

# ------------------------------------------------------------
# 8. Escritura del dataset integrado
# ------------------------------------------------------------

(
    integrado
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

print("\nDATASET INTEGRADO GENERADO:")
print(RUTA_SALIDA)

spark.stop()
