from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("01_InspeccionFuentesLATAM")
    .getOrCreate()
)

ruta_bts = "hdfs:///proyecto_latam/raw/bts/*.csv"
ruta_noaa = "hdfs:///proyecto_latam/raw/noaa/*.psv"

print("=" * 80)
print("INSPECCIÓN DE FUENTES EN HDFS CON SPARK")
print("=" * 80)

print("\nLECTURA BTS")
bts = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(ruta_bts)
)

print("Registros BTS:", bts.count())
print("Columnas BTS:", len(bts.columns))
bts.printSchema()

print("\nMuestra BTS:")
bts.select(
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepDelayMinutes",
    "DepDel15"
).show(5, truncate=False)

print("\nLECTURA NOAA")
noaa = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "|")
    .option("mode", "PERMISSIVE")
    .csv(ruta_noaa)
)

print("Registros NOAA:", noaa.count())
print("Columnas NOAA:", len(noaa.columns))
noaa.printSchema()

print("\nMuestra NOAA:")

noaa.select(
    "STATION",
    "DATE",
    "temperature",
    "wind_direction",
    "wind_speed",
    "visibility",
    "sea_level_pressure"
).show(5, truncate=False)

print("\nVALIDACIÓN DE COLUMNAS")

columnas_bts_requeridas = [
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepDelayMinutes",
    "DepDel15",
    "Distance"
]


columnas_noaa = [
    "STATION",
    "DATE",
    "temperature",
    "wind_direction",
    "wind_speed",
    "visibility",
    "sea_level_pressure"
]



for columna in columnas_bts_requeridas:
    estado = "OK" if columna in bts.columns else "FALTA"
    print(f"BTS - {columna}: {estado}")

for columna in columnas_noaa:
    estado = "OK" if columna in noaa.columns else "FALTA"
    print(f"NOAA - {columna}: {estado}")

print("\nNULOS EN VARIABLES BTS")
for columna in columnas_bts_requeridas:
    if columna in bts.columns:
        nulos = bts.filter(col(columna).isNull()).count()
        print(f"{columna}: {nulos}")

print("\nNULOS EN VARIABLES NOAA")
for columna in columnas_noaa:
    if columna in noaa.columns:
        nulos = noaa.filter(col(columna).isNull()).count()
        print(f"{columna}: {nulos}")

spark.stop()
