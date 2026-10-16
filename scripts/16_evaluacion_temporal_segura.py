"""Evaluación temporal separada con variables verificables antes de T-2 h.

Uso: spark-submit --master yarn --deploy-mode client scripts/16_evaluacion_temporal_segura.py
No modifica las salidas de los scripts 11, 13 o 15.
"""

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "hdfs:///proyecto_latam/processed/integrado"
MODEL = "hdfs:///proyecto_latam/outputs/modelos/modelo_temporal_seguro"
METRICS = "hdfs:///proyecto_latam/outputs/metricas/modelo_temporal_seguro"

spark = (SparkSession.builder.appName("16_EvaluacionTemporalSegura")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.shuffle.partitions", "48").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.parquet(INPUT)
categorical = ["Origin", "Dest", "Reporting_Airline"]
# Solo programación BTS y observaciones NOAA fechadas antes de la alerta.
# Los historiales de 05-07 quedan excluidos hasta verificar su disponibilidad.
numeric = ["scheduled_hour", "scheduled_day_of_week", "is_weekend",
           "Distance", "DistanceGroup", "temperature", "dew_point_temperature",
           "relative_humidity", "wind_direction", "wind_speed", "wind_gust",
           "visibility", "station_level_pressure", "sea_level_pressure",
           "precipitation", "weather_age_minutes", "has_weather_match"]
needed = categorical + numeric + ["FlightDate", "delay_target", "alert_time_utc",
                                  "weather_time_utc"]
missing = sorted(set(needed) - set(raw.columns))
if missing:
    raise ValueError("Faltan columnas requeridas: " + ", ".join(missing))

invalid_weather = raw.where(F.col("weather_time_utc") > F.col("alert_time_utc")).count()
if invalid_weather:
    raise ValueError(f"Hay {invalid_weather} observaciones posteriores a la alerta")

df = raw.select(*needed).withColumn("fecha", F.to_date("FlightDate"))
df = df.where(F.col("fecha").between("2026-01-01", "2026-07-31"))
df = df.where(F.col("delay_target").isin(0, 1))
for c in categorical:
    df = df.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("DESCONOCIDO")))
for c in numeric:
    df = df.withColumn(c + "_faltante", F.col(c).isNull().cast("double"))
    df = df.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
df = df.withColumn("label", F.col("delay_target").cast("double"))
feature_cols = numeric + [c + "_faltante" for c in numeric]
df = df.select("fecha", "label", *categorical, *feature_cols).cache()

train = df.where(F.col("fecha") < "2026-06-01")
validation = df.where(F.col("fecha").between("2026-06-01", "2026-06-30"))
train_final = df.where(F.col("fecha") < "2026-07-01")
test = df.where(F.col("fecha").between("2026-07-01", "2026-07-31"))
counts = [x.count() for x in (train, validation, train_final, test)]
print("FILAS train_ene_may, validacion_jun, refit_ene_jun, prueba_jul:", counts, flush=True)
if min(counts) == 0:
    raise ValueError("Una partición temporal está vacía")

def classifier(name):
    if name == "logistic_regression":
        return LogisticRegression(featuresCol="features", labelCol="label",
                                  maxIter=50, regParam=0.03)
    if name == "random_forest":
        return RandomForestClassifier(featuresCol="features", labelCol="label",
                                      numTrees=30, maxDepth=8, seed=42)
    return GBTClassifier(featuresCol="features", labelCol="label",
                         maxIter=25, maxDepth=5, stepSize=0.08, seed=42)

def pipeline(name):
    indices = [StringIndexer(inputCol=c, outputCol=c + "_index", handleInvalid="keep")
               for c in categorical]
    encoder = OneHotEncoder(inputCols=[c + "_index" for c in categorical],
                            outputCols=[c + "_vec" for c in categorical],
                            handleInvalid="keep")
    assembler = VectorAssembler(inputCols=feature_cols + [c + "_vec" for c in categorical],
                                outputCol="features", handleInvalid="keep")
    return Pipeline(stages=indices + [encoder, assembler, classifier(name)])

def score(pred, name, split, threshold):
    classified = pred.withColumn("alerta", (F.col("p") >= F.lit(threshold)).cast("int"))
    rows = classified.groupBy("label", "alerta").count().collect()
    matrix = {(int(r["label"]), int(r["alerta"])): r["count"] for r in rows}
    tn, fp, fn, tp = (matrix.get((0, 0), 0), matrix.get((0, 1), 0),
                      matrix.get((1, 0), 0), matrix.get((1, 1), 0))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return (name, split, float(threshold), int(tn), int(fp), int(fn), int(tp),
            float(precision), float(recall), float(f1),
            float(1000 * (tp + fp) / (tn + fp + fn + tp)))

results = []
choices = []
for name in ("logistic_regression", "random_forest", "gbt_classifier"):
    print("ENTRENANDO", name, flush=True)
    fitted = pipeline(name).fit(train)
    predicted = fitted.transform(validation).withColumn(
        "p", vector_to_array(F.col("probability"))[1]).cache()
    for threshold in (0.3, 0.4, 0.5, 0.6):
        row = score(predicted, name, "validacion_junio", threshold)
        results.append(row)
        choices.append(row)
        print("VALIDACION", row, flush=True)
    predicted.unpersist()

# Regla de selección declarada ANTES de observar julio.
eligible = [r for r in choices if r[7] >= 0.50 and r[8] >= 0.40]
selected = max(eligible or choices, key=lambda r: (r[9], r[7], -r[10]))
name, threshold = selected[0], selected[2]
print("SELECCION JUNIO", name, threshold, "f1", selected[9], flush=True)

final_model = pipeline(name).fit(train_final)
final_pred = final_model.transform(test).withColumn(
    "p", vector_to_array(F.col("probability"))[1]).cache()
final_row = score(final_pred, name, "prueba_julio_independiente", threshold)
results.append(final_row)
roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(final_pred)
pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR").evaluate(final_pred)
print("PRUEBA JULIO", final_row, "ROC_AUC", roc, "PR_AUC", pr, flush=True)

schema = ["modelo", "particion", "umbral", "tn", "fp", "fn", "tp",
          "precision", "recall", "f1", "alertas_por_1000"]
spark.createDataFrame(results, schema).coalesce(1).write.mode("overwrite").option(
    "header", "true").csv(METRICS)
final_model.write().overwrite().save(MODEL)
print("MODELO", MODEL, "METRICAS", METRICS, flush=True)
final_pred.unpersist()
df.unpersist()
spark.stop()
