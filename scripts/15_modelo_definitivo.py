from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, udf
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (
    SparkSession.builder
    .appName("15_Modelo_Definitivo_LATAM")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .getOrCreate()
)

RUTA_DATOS = "hdfs:///proyecto_latam/processed/integrado"
RUTA_SALIDA = "hdfs:///proyecto_latam/outputs/modelos/modelo_definitivo"
RUTA_METRICAS = "hdfs:///proyecto_latam/outputs/metricas/modelo_definitivo"

print("=" * 80)
print("MODELO DEFINITIVO: COMPARACION AUTOMATICA")
print("=" * 80)

df = spark.read.parquet(RUTA_DATOS)

# Renombrar columnas duplicadas del Parquet
nombres_usados = set()
nombres_unicos = []

for nombre in df.columns:
    nuevo = nombre
    contador = 1

    while nuevo in nombres_usados:
        nuevo = f"{nombre}_duplicada_{contador}"
        contador += 1

    nombres_usados.add(nuevo)
    nombres_unicos.append(nuevo)

df = df.toDF(*nombres_unicos)

features_numericas = [
    "scheduled_hour",
    "scheduled_day_of_week",
    "scheduled_month",
    "is_weekend",
    "Distance",
    "DistanceGroup",
    "previous_flight_delay",
    "turnaround_minutes",
    "aircraft_previous_flights",
    "aircraft_recent_avg_delay",
    "aircraft_recent_max_delay",
    "aircraft_previous_delay_rate",
    "aircraft_previous_delayed_count",
    "airport_departures_previous_1h",
    "airport_departures_previous_2h",
    "airport_active_flights_same_hour",
    "airport_previous_delay_rate",
    "airport_previous_avg_delay",
    "airport_hour_delay_rate",
    "airline_previous_avg_delay",
    "airline_previous_delay_rate",
    "airline_previous_flight_count",
    "route_previous_avg_delay",
    "route_previous_delay_rate",
    "route_previous_flight_count",
    "airport_airline_previous_delay_rate",
    "airline_airport_previous_flight_count",
    "airport_hour_previous_delay_rate",
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
    "weather_age_minutes",
    "has_weather_match",
    "has_missing_weather"
]

features_numericas = [
    c for c in features_numericas if c in df.columns
]

columnas_categoricas = [
    c for c in ["Origin", "Dest", "Reporting_Airline"]
    if c in df.columns
]

columnas_necesarias = [
    "FlightDate",
    "delay_target"
] + columnas_categoricas + features_numericas

columnas_necesarias = [
    c for c in columnas_necesarias if c in df.columns
]

df = df.select(*columnas_necesarias)

# Conversión y tratamiento de nulos
for c in features_numericas:
    df = df.withColumn(
        c,
        when(col(c).isNull(), lit(0.0))
        .otherwise(col(c).cast("double"))
    )

for c in columnas_categoricas:
    df = df.withColumn(
        c,
        when(col(c).isNull(), lit("DESCONOCIDO"))
        .otherwise(col(c).cast("string"))
    )

df = df.withColumn(
    "label",
    col("delay_target").cast("double")
).drop("delay_target")

# Separación temporal correcta
df = df.withColumn("fecha_modelo", to_date(col("FlightDate")))

entrenamiento = df.filter(
    col("fecha_modelo") < lit("2026-07-01")
)

prueba = df.filter(
    col("fecha_modelo") >= lit("2026-07-01")
)

print("Filas de entrenamiento:", entrenamiento.count())
print("Filas de prueba:", prueba.count())
print("Variables numericas:", len(features_numericas))
print("Variables categoricas:", columnas_categoricas)

# Balance de clases
entrenamiento = entrenamiento.withColumn(
    "class_weight",
    when(col("label") == 1.0, lit(1.8))
    .otherwise(lit(1.0))
)

indexadores = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_index",
        handleInvalid="keep"
    )
    for c in columnas_categoricas
]

columnas_indexadas = [c + "_index" for c in columnas_categoricas]
columnas_vector = [c + "_vec" for c in columnas_categoricas]

encoder = OneHotEncoder(
    inputCols=columnas_indexadas,
    outputCols=columnas_vector,
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=features_numericas + columnas_vector,
    outputCol="features",
    handleInvalid="keep"
)

modelos = {
    "logistic_regression": LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=80,
        regParam=0.03,
        elasticNetParam=0.0
    ),

    "random_forest": RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        numTrees=60,
        maxDepth=12,
        seed=42
    ),

    "gbt_classifier": GBTClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=40,
        maxDepth=7,
        stepSize=0.08,
        seed=42
    )
}

probabilidad_positiva = udf(
    lambda vector: float(vector[1]),
    DoubleType()
)

evaluador_roc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

evaluador_pr = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

resultados = []
mejor_modelo = None
mejor_nombre = None
mejor_f1 = -1.0
mejor_umbral = None

for nombre_modelo, clasificador in modelos.items():

    print("\n" + "=" * 80)
    print("ENTRENANDO:", nombre_modelo)
    print("=" * 80)

    pipeline = Pipeline(
        stages=indexadores + [encoder, assembler, clasificador]
    )

    modelo = pipeline.fit(entrenamiento)
    predicciones = modelo.transform(prueba)

    predicciones = predicciones.withColumn(
        "probabilidad_demora",
        probabilidad_positiva(col("probability"))
    )

    roc_auc = evaluador_roc.evaluate(predicciones)
    pr_auc = evaluador_pr.evaluate(predicciones)

    for umbral in [0.30, 0.40, 0.50, 0.60]:

        evaluadas = predicciones.withColumn(
            "alerta",
            when(
                col("probabilidad_demora") >= lit(umbral),
                lit(1.0)
            ).otherwise(lit(0.0))
        )

        matriz = (
            evaluadas
            .groupBy("label", "alerta")
            .count()
            .collect()
        )

        tn = fp = fn = tp = 0

        for fila in matriz:
            etiqueta = float(fila["label"])
            alerta = float(fila["alerta"])
            cantidad = int(fila["count"])

            if etiqueta == 0.0 and alerta == 0.0:
                tn = cantidad
            elif etiqueta == 0.0 and alerta == 1.0:
                fp = cantidad
            elif etiqueta == 1.0 and alerta == 0.0:
                fn = cantidad
            elif etiqueta == 1.0 and alerta == 1.0:
                tp = cantidad

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        total_prueba = tp + tn + fp + fn
        alertas = tp + fp
        alertas_1000 = (
            alertas / total_prueba * 1000
            if total_prueba > 0
            else 0.0
        )

        resultados.append((
            nombre_modelo,
            float(umbral),
            float(roc_auc),
            float(pr_auc),
            int(tn),
            int(fp),
            int(fn),
            int(tp),
            float(precision),
            float(recall),
            float(f1),
            int(alertas),
            float(alertas_1000)
        ))

        print(
            nombre_modelo,
            "umbral=", umbral,
            "precision=", round(precision, 4),
            "recall=", round(recall, 4),
            "f1=", round(f1, 4),
            "alertas_1000=", round(alertas_1000, 2)
        )

        # Criterio de selección
        if (
            precision >= 0.50
            and recall >= 0.40
            and f1 > mejor_f1
        ):
            mejor_f1 = f1
            mejor_modelo = modelo
            mejor_nombre = nombre_modelo
            mejor_umbral = umbral

# Si ningún modelo cumple el filtro, seleccionar el máximo F1
if mejor_modelo is None:
    candidato = max(resultados, key=lambda x: x[10])
    mejor_nombre = candidato[0]
    mejor_umbral = candidato[1]
    mejor_f1 = candidato[10]

    clasificador = modelos[mejor_nombre]

    pipeline = Pipeline(
        stages=indexadores + [encoder, assembler, clasificador]
    )

    mejor_modelo = pipeline.fit(entrenamiento)

print("\n" + "=" * 80)
print("MEJOR MODELO SELECCIONADO")
print("=" * 80)
print("Modelo:", mejor_nombre)
print("Umbral:", mejor_umbral)
print("F1:", round(mejor_f1, 4))

# Guardar el mejor modelo
mejor_modelo.write().overwrite().save(RUTA_SALIDA)

# Guardar todas las métricas
columnas_resultados = [
    "modelo",
    "umbral",
    "roc_auc",
    "pr_auc",
    "tn",
    "fp",
    "fn",
    "tp",
    "precision",
    "recall",
    "f1",
    "alertas",
    "alertas_por_1000"
]

df_resultados = spark.createDataFrame(
    resultados,
    columnas_resultados
)

df_resultados.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(RUTA_METRICAS)

print("MODELO GUARDADO:", RUTA_SALIDA)
print("METRICAS GUARDADAS:", RUTA_METRICAS)

spark.stop()
