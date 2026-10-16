from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PruebaSparkYARNLatam")
    .getOrCreate()
)

datos = [
    (1, "BTS"),
    (2, "NOAA"),
    (3, "LATAM")
]

df = spark.createDataFrame(datos, ["id", "fuente"])

print("Registros procesados:", df.count())
df.show()

spark.stop()
