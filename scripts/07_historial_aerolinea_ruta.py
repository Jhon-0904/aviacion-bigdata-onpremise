from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    hour,
    unix_timestamp
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("07_HistorialAerolineaRuta")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/staging/bts_congestion"
RUTA_SALIDA = "hdfs:///proyecto_latam/staging/bts_operativo"

print("=" * 80)
print("MODULO 07 - HISTORIAL DE AEROLINEA Y RUTA")
print("=" * 80)

vuelos = spark.read.parquet(RUTA_ENTRADA)

print("Registros de entrada:", vuelos.count())

vuelos = vuelos.withColumn(
    "scheduled_dep_epoch",
    unix_timestamp(col("scheduled_dep_utc"))
)

vuelos = vuelos.withColumn(
    "scheduled_hour",
    hour(col("scheduled_dep_local"))
)

# Historial por aerolínea
ventana_aerolinea = (
    Window
    .partitionBy("Reporting_Airline")
    .orderBy("scheduled_dep_epoch")
    .rowsBetween(Window.unboundedPreceding, -1)
)

# Historial por ruta
ventana_ruta = (
    Window
    .partitionBy("Origin", "Dest")
    .orderBy("scheduled_dep_epoch")
    .rowsBetween(Window.unboundedPreceding, -1)
)

# Historial aeropuerto-aerolínea
ventana_aeropuerto_aerolinea = (
    Window
    .partitionBy("Origin", "Reporting_Airline")
    .orderBy("scheduled_dep_epoch")
    .rowsBetween(Window.unboundedPreceding, -1)
)

# Historial por aeropuerto y franja horaria
ventana_aeropuerto_hora = (
    Window
    .partitionBy("Origin", "scheduled_hour")
    .orderBy("scheduled_dep_epoch")
    .rowsBetween(Window.unboundedPreceding, -1)
)

vuelos = (
    vuelos
    .withColumn(
        "airline_previous_avg_delay",
        avg("delay_minutes_target").over(ventana_aerolinea)
    )
    .withColumn(
        "airline_previous_delay_rate",
        avg("delay_target").over(ventana_aerolinea)
    )
    .withColumn(
        "airline_previous_flight_count",
        count("*").over(ventana_aerolinea)
    )
    .withColumn(
        "route_previous_avg_delay",
        avg("delay_minutes_target").over(ventana_ruta)
    )
    .withColumn(
        "route_previous_delay_rate",
        avg("delay_target").over(ventana_ruta)
    )
    .withColumn(
        "route_previous_flight_count",
        count("*").over(ventana_ruta)
    )
    .withColumn(
        "airport_airline_previous_delay_rate",
        avg("delay_target").over(
            ventana_aeropuerto_aerolinea
        )
    )
    .withColumn(
        "airline_airport_previous_flight_count",
        count("*").over(
            ventana_aeropuerto_aerolinea
        )
    )
    .withColumn(
        "airport_hour_previous_delay_rate",
        avg("delay_target").over(
            ventana_aeropuerto_hora
        )
    )
)

print("Registros finales:", vuelos.count())
print("Columnas finales:", len(vuelos.columns))

variables = [
    "airline_previous_avg_delay",
    "airline_previous_delay_rate",
    "airline_previous_flight_count",
    "route_previous_avg_delay",
    "route_previous_delay_rate",
    "route_previous_flight_count",
    "airport_airline_previous_delay_rate",
    "airline_airport_previous_flight_count",
    "airport_hour_previous_delay_rate"
]

print("\nVARIABLES HISTORICAS")

for variable in variables:
    if variable in vuelos.columns:
        print(variable + ": OK")
    else:
        print(variable + ": FALTA")

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
