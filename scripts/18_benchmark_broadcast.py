from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BTS = "hdfs:///proyecto_latam/raw/bts"
MAP = "hdfs:///proyecto_latam/reference/mapeo_aeropuerto_estacion.csv"
OUT = "hdfs:///proyecto_latam/outputs/metricas/optimizacion_broadcast"

spark = (
    SparkSession.builder
    .appName("18_BenchmarkBroadcastRutaA")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "48")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bts = (
    spark.read.option("header", True).csv(BTS)
    .select(F.trim("Origin").alias("Origin"))
)
mapeo = (
    spark.read.option("header", True).csv(MAP)
    .select(F.col("origin").alias("map_origin"), "station_id")
)

def medir(nombre, tabla_mapeo):
    unidos = bts.join(
        tabla_mapeo,
        bts.Origin == tabla_mapeo.map_origin,
        "inner"
    )
    consulta = (
        unidos.groupBy("station_id")
        .agg(F.count("*").alias("vuelos"))
        .orderBy("station_id")
    )
    plan = consulta._jdf.queryExecution().executedPlan().toString()
    inicio = perf_counter()
    resultado = [
        (fila["station_id"], int(fila["vuelos"]))
        for fila in consulta.collect()
    ]
    segundos = perf_counter() - inicio
    tipo = (
        "BroadcastHashJoin" if "BroadcastHashJoin" in plan
        else "SortMergeJoin" if "SortMergeJoin" in plan
        else "OTRO"
    )
    print(
        nombre, "SEGUNDOS", round(segundos, 3),
        "FILAS", sum(cantidad for _, cantidad in resultado),
        "PLAN_JOIN", tipo,
        flush=True
    )
    return resultado, segundos, tipo

base, tiempo_base, plan_base = medir("sin_broadcast", mapeo)
optimizado, tiempo_opt, plan_opt = medir(
    "con_broadcast", F.broadcast(mapeo)
)

if base != optimizado:
    raise ValueError("Las dos variantes produjeron resultados distintos")
if plan_opt != "BroadcastHashJoin":
    raise ValueError("Spark no aplicó BroadcastHashJoin")

filas = [
    ("sin_broadcast", float(tiempo_base), sum(n for _, n in base)),
    ("con_broadcast", float(tiempo_opt), sum(n for _, n in optimizado))
]
(
    spark.createDataFrame(
        filas, ["variante", "segundos", "filas_con_mapeo"]
    )
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(OUT)
)

print("RESULTADO_3_6", OUT, flush=True)
spark.stop()
