from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("03_ValidarMapeoBTSNOAA")
    .getOrCreate()
)

ruta_bts = "hdfs:///proyecto_latam/raw/bts"
ruta_mapeo = "hdfs:///proyecto_latam/reference/mapeo_aeropuerto_estacion.csv"

bts = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(ruta_bts)
    .alias("b")
)

mapeo = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ruta_mapeo)
    .alias("m")
)

print("=" * 80)
print("VALIDACIÓN DEL MAPEO BTS–NOAA")
print("=" * 80)

print("\nConteo de vuelos por aeropuerto mapeado:")

conteo = (
    bts.join(
        mapeo,
        col("b.Origin") == col("m.origin"),
        "inner"
    )
    .groupBy(
        col("m.origin").alias("origin"),
        col("m.station_id").alias("station_id"),
        col("m.time_zone").alias("time_zone"),
        col("m.station_name").alias("station_name")
    )
    .agg(count("*").alias("vuelos_bts"))
    .orderBy("origin")
)

conteo.show(20, False)

print("\nAeropuertos del mapeo sin vuelos en BTS:")

origenes_bts = (
    bts.select(col("Origin").alias("origin_bts"))
    .distinct()
)

sin_vuelos = (
    mapeo.join(
        origenes_bts,
        col("m.origin") == col("origin_bts"),
        "left_anti"
    )
    .select(
        col("m.origin").alias("origin"),
        col("m.station_id").alias("station_id"),
        col("m.station_name").alias("station_name")
    )
)

sin_vuelos.show(20, False)

print("\nResumen:")
print("Vuelos BTS totales:", bts.count())
print("Aeropuertos en el mapeo:", mapeo.count())
print(
    "Aeropuertos con correspondencia:",
    conteo.select("origin").distinct().count()
)
print("Aeropuertos sin correspondencia:", sin_vuelos.count())

spark.stop()
