from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import Vector

spark = (
    SparkSession.builder
    .appName("14_ModeloMejorado")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "48")
    .getOrCreate()
)

RUTA = "hdfs:///proyecto_latam/processed/integrado"

df = spark.read.parquet(RUTA)

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
    c for c in features_numericas
    if c in df.columns
]

df = df.select(
    "Origin",
    "Dest",
    "Reporting_Airline",
    "scheduled_month",
    "delay_target",
    *features_numericas
)

for c in features_numericas:
    df = df.withColumn(
        c,
        when(col(c).isNull(), 0.0)
        .otherwise(col(c).cast("double"))
    )

df = df.withColumn(
    "label",
    col("delay_target").cast("double")
).drop("delay_target")

# Ponderación de clases
df = df.withColumn(
    "class_weight",
    when(col("label") == 1.0, 1.8).otherwise(1.0)
)

entrenamiento = df.filter(col("scheduled_month") <= 6)
prueba = df.filter(col("scheduled_month") == 7)

indexadores = [
    StringIndexer(
        inputCol=c,
        outputCol=c + "_index",
        handleInvalid="keep"
    )
    for c in ["Origin", "Dest", "Reporting_Airline"]
]

encoder = OneHotEncoder(
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

assembler = VectorAssembler(
    inputCols=[
        *features_numericas,
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
    weightCol="class_weight",
    maxIter=60,
    regParam=0.03,
    elasticNetParam=0.0
)

pipeline = Pipeline(
    stages=indexadores + [encoder, assembler, clasificador]
)

print("Entrenando modelo mejorado...")
modelo = pipeline.fit(entrenamiento)

pred = modelo.transform(prueba)

# Extraer probabilidad de la clase positiva
probabilidad_positiva = udf(
    lambda v: float(v[1]),
    DoubleType()
)

pred = pred.withColumn(
    "probabilidad_demora",
    probabilidad_positiva(col("probability"))
)

# Umbral operativo ajustado
UMBRAL = 0.35

pred = pred.withColumn(
    "alerta",
    when(
        col("probabilidad_demora") >= UMBRAL,
        1.0
    ).otherwise(0.0)
)

conteos = (
    pred
    .groupBy("label", "alerta")
    .count()
    .collect()
)

tn = fp = fn = tp = 0

for fila in conteos:
    label = float(fila["label"])
    alerta = float(fila["alerta"])
    cantidad = int(fila["count"])

    if label == 0.0 and alerta == 0.0:
        tn = cantidad
    elif label == 0.0 and alerta == 1.0:
        fp = cantidad
    elif label == 1.0 and alerta == 0.0:
        fn = cantidad
    elif label == 1.0 and alerta == 1.0:
        tp = cantidad

precision = tp / (tp + fp) if (tp + fp) else 0
recall = tp / (tp + fn) if (tp + fn) else 0
f1 = (
    2 * precision * recall / (precision + recall)
    if (precision + recall) else 0
)

alertas = tp + fp
total = pred.count()

print("\nRESULTADOS MODELO MEJORADO")
print("Umbral:", UMBRAL)
print("Verdaderos negativos:", tn)
print("Falsos positivos:", fp)
print("Falsos negativos:", fn)
print("Verdaderos positivos:", tp)
print("Precision positiva:", round(precision, 4))
print("Recall positivo:", round(recall, 4))
print("F1 positivo:", round(f1, 4))
print("Alertas generadas:", alertas)
print(
    "Alertas por cada 1000 vuelos:",
    round((alertas / total) * 1000, 2)
)

modelo.write().overwrite().save(
    "hdfs:///proyecto_latam/outputs/modelos/modelo_mejorado"
)

spark.stop()
