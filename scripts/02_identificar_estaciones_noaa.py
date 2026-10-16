from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count

spark = (
    SparkSession.builder
    .appName("02_IdentificarEstacionesNOAA")
    .getOrCreate()
)

noaa = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("inferSchema", "true")
    .csv("hdfs:///proyecto_latam/raw/noaa/*.psv")
)

resumen = (
    noaa.groupBy(
        "STATION",
        "Station_name",
        "LATITUDE",
        "LONGITUDE",
        "ELEVATION"
    )
    .agg(
        count("*").alias("observaciones"),
        min("DATE").alias("fecha_minima"),
        max("DATE").alias("fecha_maxima")
    )
    .orderBy("STATION")
)

print("=" * 100)
print("CORRESPONDENCIA Y COBERTURA DE ESTACIONES NOAA")
print("=" * 100)

resumen.show(20, truncate=False)

spark.stop()
