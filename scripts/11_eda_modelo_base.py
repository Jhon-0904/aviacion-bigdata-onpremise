from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    when,
    round as spark_round
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator
)

spark = (
    SparkSession.builder
    .appName("11_EDA_ModeloBase")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/processed/integrado"
RUTA_EDA = "hdfs:///proyecto_latam/outputs/eda"
RUTA_METRICAS = "hdfs:///proyecto_latam/outputs/metricas/modelo_base"
RUTA_MODELO = "hdfs:///proyecto_latam/outputs/modelos/logistic_regression_base"

print("=" * 80)
print("MODULO 11 - EDA Y MODELO BASE")
print("=" * 80)

# ------------------------------------------------------------
# 1. Lectura del dataset integrado
# ------------------------------------------------------------

df = spark.read.parquet(RUTA_ENTRADA)

print("Registros totales:", df.count())
print("Columnas totales:", len(df.columns))

# ------------------------------------------------------------
# 2. Análisis exploratorio
# ------------------------------------------------------------

print("\nEDA 1 - DEMORAS POR AEROPUERTO")

eda_aeropuerto = (
    df.groupBy("Origin")
    .agg(
        count("*").alias("total_vuelos"),
        avg("delay_target").alias("tasa_demora"),
        avg("delay_minutes_target").alias("demora_promedio")
    )
    .withColumn(
        "tasa_demora_porcentaje",
        spark_round(col("tasa_demora") * 100, 2)
    )
    .orderBy(col("tasa_demora").desc())
)

eda_aeropuerto.show(20, False)

(
    eda_aeropuerto
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(RUTA_EDA + "/demora_por_aeropuerto")
)

print("\nEDA 2 - DEMORAS POR HORA")

eda_hora = (
    df.groupBy("scheduled_hour")
    .agg(
        count("*").alias("total_vuelos"),
        avg("delay_target").alias("tasa_demora"),
        avg("delay_minutes_target").alias("demora_promedio")
    )
    .withColumn(
        "tasa_demora_porcentaje",
        spark_round(col("tasa_demora") * 100, 2)
    )
    .orderBy("scheduled_hour")
)

eda_hora.show(30, False)

(
    eda_hora
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(RUTA_EDA + "/demora_por_hora")
)

print("\nEDA 3 - VISIBILIDAD Y DEMORA")

eda_visibilidad = (
    df.withColumn(
        "visibility_group",
        when(col("visibility").isNull(), "sin_dato")
        .when(col("visibility") < 1000, "menos_1000_m")
        .when(col("visibility") < 5000, "1000_5000_m")
        .when(col("visibility") < 10000, "5000_10000_m")
        .otherwise("mayor_10000_m")
    )
    .groupBy("visibility_group")
    .agg(
        count("*").alias("total_vuelos"),
        avg("delay_target").alias("tasa_demora"),
        avg("delay_minutes_target").alias("demora_promedio")
    )
    .withColumn(
        "tasa_demora_porcentaje",
        spark_round(col("tasa_demora") * 100, 2)
    )
)

eda_visibilidad.show(20, False)

(
    eda_visibilidad
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(RUTA_EDA + "/demora_por_visibilidad")
)

# ------------------------------------------------------------
# 3. Preparación del conjunto de modelado
# ------------------------------------------------------------

columnas_numericas = [
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

columnas_numericas = [
    c for c in columnas_numericas
    if c in df.columns
]

df_modelo = df.select(
    "Origin",
    "Dest",
    "Reporting_Airline",
    "delay_target",
    *columnas_numericas
)

# Imputación simple para el modelo base.
# La imputación se realiza dentro del conjunto de modelado.
for c in columnas_numericas:
    df_modelo = df_modelo.withColumn(
        c,
        when(col(c).isNull(), 0.0)
        .otherwise(col(c).cast("double"))
    )

df_modelo = df_modelo.withColumn(
    "label",
    col("delay_target").cast("double")
).drop("delay_target")

# ------------------------------------------------------------
# 4. División temporal
# ------------------------------------------------------------

entrenamiento = df_modelo.filter(
    col("scheduled_month") <= 6
)

prueba = df_modelo.filter(
    col("scheduled_month") == 7
)

print("\nDIVISION TEMPORAL")
print("Registros de entrenamiento:", entrenamiento.count())
print("Registros de prueba:", prueba.count())

# ------------------------------------------------------------
# 5. Pipeline de clasificación
# ------------------------------------------------------------

indexadores = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_index",
        handleInvalid="keep"
    )
    for c in ["Origin", "Dest", "Reporting_Airline"]
]

codificador = OneHotEncoder(
    inputCols=[
        "Origin_index",
        "Dest_index",
        "Reporting_Airline_index"
    ],
    outputCols=[
        "Origin_vec",
        "Dest_vec",
        "Reporting_Airline_vec"
    ]
)

ensamblador = VectorAssembler(
    inputCols=[
        *columnas_numericas,
        "Origin_vec",
        "Dest_vec",
        "Reporting_Airline_vec"
    ],
    outputCol="features",
    handleInvalid="keep"
)

clasificador = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    maxIter=40,
    regParam=0.05,
    elasticNetParam=0.0
)

pipeline = Pipeline(
    stages=indexadores + [
        codificador,
        ensamblador,
        clasificador
    ]
)

# ------------------------------------------------------------
# 6. Entrenamiento
# ------------------------------------------------------------

print("\nENTRENANDO MODELO BASE")

modelo = pipeline.fit(entrenamiento)

predicciones = modelo.transform(prueba)

# ------------------------------------------------------------
# 7. Evaluación
# ------------------------------------------------------------

evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

evaluator_precision = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

evaluator_recall = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

evaluator_auc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

f1 = evaluator_f1.evaluate(predicciones)
precision = evaluator_precision.evaluate(predicciones)
recall = evaluator_recall.evaluate(predicciones)
accuracy = evaluator_accuracy.evaluate(predicciones)
roc_auc = evaluator_auc.evaluate(predicciones)

print("\nRESULTADOS DEL MODELO BASE")
print("F1:", round(f1, 4))
print("Precision ponderada:", round(precision, 4))
print("Recall ponderado:", round(recall, 4))
print("Accuracy:", round(accuracy, 4))
print("ROC-AUC:", round(roc_auc, 4))



print("\nMATRIZ DE CONFUSION")

matriz = (
    predicciones
    .groupBy("label", "prediction")
    .count()
    .orderBy("label", "prediction")
)

matriz.show()

# Métricas específicas de la clase positiva: DepDel15 = 1
evaluator_precision_positiva = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="precisionByLabel",
    metricLabel=1.0
)

evaluator_recall_positivo = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="recallByLabel",
    metricLabel=1.0
)

evaluator_f1_positivo = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="fMeasureByLabel",
    metricLabel=1.0
)

precision_positiva = evaluator_precision_positiva.evaluate(
    predicciones
)

recall_positivo = evaluator_recall_positivo.evaluate(
    predicciones
)

f1_positivo = evaluator_f1_positivo.evaluate(
    predicciones
)

print("\nMETRICAS DE LA CLASE POSITIVA")
print(
    "Precision positiva:",
    round(precision_positiva, 4)
)
print(
    "Recall positivo:",
    round(recall_positivo, 4)
)
print(
    "F1 positivo:",
    round(f1_positivo, 4)
)

# Conteo de falsos positivos y falsos negativos
conteos = (
    predicciones
    .groupBy("label", "prediction")
    .count()
    .collect()
)

tn = 0
fp = 0
fn = 0
tp = 0

for fila in conteos:
    etiqueta = float(fila["label"])
    prediccion = float(fila["prediction"])
    cantidad = int(fila["count"])

    if etiqueta == 0.0 and prediccion == 0.0:
        tn = cantidad
    elif etiqueta == 0.0 and prediccion == 1.0:
        fp = cantidad
    elif etiqueta == 1.0 and prediccion == 0.0:
        fn = cantidad
    elif etiqueta == 1.0 and prediccion == 1.0:
        tp = cantidad

total_alertas = tp + fp
alertas_por_1000 = (
    total_alertas / prueba.count()
) * 1000

print("\nINDICADORES OPERATIVOS")
print("Verdaderos negativos:", tn)
print("Falsos positivos:", fp)
print("Falsos negativos:", fn)
print("Verdaderos positivos:", tp)
print("Alertas generadas:", total_alertas)
print(
    "Alertas por cada 1000 vuelos:",
    round(alertas_por_1000, 2)
)


# ------------------------------------------------------------
# 8. Guardado del modelo y métricas
# ------------------------------------------------------------

modelo.write().overwrite().save(RUTA_MODELO)

metricas = spark.createDataFrame(
    [
        (
            "logistic_regression_base",
            float(f1),
            float(precision),
            float(recall),
            float(accuracy),
            float(roc_auc),
            int(entrenamiento.count()),
            int(prueba.count())
        )
    ],
    [
        "modelo",
        "f1",
        "precision_ponderada",
        "recall_ponderado",
        "accuracy",
        "roc_auc",
        "filas_entrenamiento",
        "filas_prueba"
    ]
)

(
    metricas
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(RUTA_METRICAS)
)

print("\nMODELO GUARDADO:")
print(RUTA_MODELO)

print("\nMETRICAS GUARDADAS:")
print(RUTA_METRICAS)

spark.stop()
