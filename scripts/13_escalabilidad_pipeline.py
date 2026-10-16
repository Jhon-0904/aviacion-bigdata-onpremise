from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col
import time
import resource

spark = (
    SparkSession.builder
    .appName("13_EscalabilidadPipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "96")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/processed/integrado"

df_base = spark.read.parquet(RUTA_ENTRADA)

print("=" * 80)
print("PRUEBA DE ESCALABILIDAD DEL PIPELINE PROCESADO")
print("=" * 80)

filas_base = df_base.count()

print("Filas base:", filas_base)

resultados = []

for factor in [1, 10, 100]:

    print("\nEjecutando factor:", factor)

    inicio = time.time()

    if factor == 1:
        df_prueba = df_base
    else:
        copias = [df_base for _ in range(factor)]
        df_prueba = copias[0]

        for copia in copias[1:]:
            df_prueba = df_prueba.unionByName(copia)

    filas = df_prueba.count()

    resumen = (
        df_prueba
        .groupBy("Origin")
        .agg(
            count("*").alias("vuelos"),
            avg("delay_target").alias("tasa_demora")
        )
        .collect()
    )

    tiempo = time.time() - inicio
    memoria_kb = resource.getrusage(
        resource.RUSAGE_SELF
    ).ru_maxrss

    memoria_gb = memoria_kb / (1024 * 1024)

    resultados.append(
        (
            factor,
            filas,
            round(tiempo, 2),
            round(memoria_gb, 3),
            len(resumen)
        )
    )

    print("Factor:", factor)
    print("Filas procesadas:", filas)
    print("Tiempo segundos:", round(tiempo, 2))
    print("Memoria GB:", round(memoria_gb, 3))
    print("Aeropuertos agregados:", len(resumen))

    if factor > 1:
        df_prueba.unpersist()

print("\nRESULTADOS FINALES")

for fila in resultados:
    print(
        "factor=", fila[0],
        "| filas=", fila[1],
        "| tiempo_segundos=", fila[2],
        "| memoria_gb=", fila[3],
        "| aeropuertos=", fila[4]
    )

salida = spark.createDataFrame(
    resultados,
    [
        "factor",
        "filas_procesadas",
        "tiempo_segundos",
        "memoria_gb",
        "aeropuertos_agregados"
    ]
)

salida.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(
    "hdfs:///proyecto_latam/outputs/metricas/escalabilidad"
)

print("\nEVIDENCIA GUARDADA EN:")
print("hdfs:///proyecto_latam/outputs/metricas/escalabilidad")

spark.stop()
