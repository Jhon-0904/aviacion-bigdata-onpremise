"""Modelo dimensional descriptivo sobre el Parquet real; Spark sobre YARN."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "hdfs:///proyecto_latam/processed/integrado"
MAP = "hdfs:///proyecto_latam/reference/mapeo_aeropuerto_estacion.csv"
OUT = "hdfs:///proyecto_latam/consumo"

spark = (SparkSession.builder.appName("17_CapaConsumo")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.shuffle.partitions", "48").getOrCreate())

df = spark.read.parquet(BASE)
keys = ["FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
        "Tail_Number", "Origin", "Dest", "CRSDepTime"]
missing = sorted(set(keys + ["delay_target", "scheduled_dep_utc"]) - set(df.columns))
if missing:
    raise ValueError("Faltan columnas: " + ", ".join(missing))

mapping = (spark.read.option("header", True).csv(MAP)
           .select(F.col("origin").alias("airport_code"), "station_id", "time_zone")
           .dropDuplicates(["airport_code"]))
airports = (df.select(F.col("Origin").alias("airport_code"))
            .union(df.select(F.col("Dest").alias("airport_code")))
            .distinct().join(F.broadcast(mapping), "airport_code", "left"))
airports.write.mode("overwrite").parquet(OUT + "/dim_aeropuerto")

(df.select(F.col("Reporting_Airline").alias("airline_code")).distinct()
 .write.mode("overwrite").parquet(OUT + "/dim_aerolinea"))

(df.select(F.to_date("FlightDate").alias("fecha")).distinct()
 .withColumn("anio", F.year("fecha"))
 .withColumn("mes", F.month("fecha"))
 .withColumn("dia_semana", F.dayofweek("fecha"))
 .write.mode("overwrite").parquet(OUT + "/dim_fecha"))

# Granularidad: un vuelo programado, identificado por los campos BTS de abajo.
# Matrícula solo interviene en la clave técnica, no se publica en el hecho.
fact = (df.select(
    F.sha2(F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit(""))
                              for k in keys]), 256).alias("flight_key"),
    F.to_date("FlightDate").alias("fecha"),
    F.col("Origin").alias("origin_code"),
    F.col("Dest").alias("dest_code"),
    F.col("Reporting_Airline").alias("airline_code"),
    "scheduled_dep_utc", "alert_time_utc",
    F.col("Distance").cast("double").alias("distance_miles"),
    F.col("delay_target").cast("int").alias("delayed_15"),
    F.col("delay_minutes_target").cast("double").alias("delay_minutes"),
    F.col("has_weather_match").cast("int").alias("weather_match"))
 .withColumn("anio", F.year("fecha"))
 .withColumn("mes", F.month("fecha")))

total = fact.count()
distinct = fact.select("flight_key").distinct().count()
if total != distinct:
    raise ValueError(f"La clave del hecho no es única: {total - distinct} duplicados")

fact.write.mode("overwrite").partitionBy("anio", "mes").parquet(OUT + "/fact_vuelo")

monthly = (fact.groupBy("anio", "mes", "origin_code")
           .agg(F.count("*").alias("vuelos"),
                F.sum("delayed_15").alias("vuelos_demorados_15"),
                F.avg("delayed_15").alias("tasa_demora_15"),
                F.avg("delay_minutes").alias("demora_media_minutos"),
                F.avg("weather_match").alias("cobertura_clima")))
monthly.write.mode("overwrite").parquet(OUT + "/kpi_aeropuerto_mes")
print("CAPA CONSUMO GENERADA", OUT, "FILAS HECHO", total, flush=True)
print("DIMENSIONES aeropuerto, aerolinea, fecha; HECHO vuelo; KPI aeropuerto_mes", flush=True)
spark.stop()
