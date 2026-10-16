from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    min as spark_min,
    max as spark_max,
    avg,
    lit
)
import json

spark = (
    SparkSession.builder
    .appName("10_ControlCalidad")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

RUTA_ENTRADA = "hdfs:///proyecto_latam/processed/integrado"

df = spark.read.parquet(RUTA_ENTRADA)

total = df.count()

print("=" * 80)
print("MODULO 10 - CONTROL DE CALIDAD")
print("=" * 80)

print("Total de registros:", total)
print("Total de columnas:", len(df.columns))

# Completitud
variables_criticas = [
    "Origin",
    "Dest",
    "scheduled_dep_utc",
    "alert_time_utc",
    "delay_target",
    "temperature",
    "relative_humidity",
    "wind_speed",
    "visibility",
    "weather_time_utc",
    "weather_age_minutes",
    "airport_departures_previous_1h",
    "airport_departures_previous_2h",
    "airline_previous_delay_rate",
    "route_previous_delay_rate"
]

print("\nCOMPLETITUD DE VARIABLES")

completitud = []

for variable in variables_criticas:
    if variable in df.columns:
        validos = df.filter(col(variable).isNotNull()).count()
        porcentaje = (validos / total) * 100

        print(
            f"{variable}: {validos}/{total} "
            f"({porcentaje:.2f}%)"
        )

        completitud.append({
            "variable": variable,
            "registros_validos": validos,
            "total_registros": total,
            "completitud_porcentaje": round(porcentaje, 2)
        })

# Correspondencia meteorológica
print("\nCORRESPONDENCIA METEOROLOGICA")

if "has_weather_match" in df.columns:
    df.groupBy("has_weather_match").count().show()

    con_clima = df.filter(
        col("has_weather_match") == 1
    ).count()

    porcentaje_clima = (con_clima / total) * 100

    print("Vuelos con clima:", con_clima)
    print(
        "Porcentaje con clima:",
        round(porcentaje_clima, 2)
    )

# Duplicados
columnas_clave = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime"
]

total_clave = df.select(*columnas_clave).count()
total_distinto = df.select(*columnas_clave).distinct().count()
duplicados = total_clave - total_distinto

print("\nDUPLICADOS")
print("Duplicados por clave de vuelo:", duplicados)

# Rangos
print("\nRANGOS Y COHERENCIA")

df.select(
    spark_min("delay_minutes_target").alias(
        "delay_minimo"
    ),
    spark_max("delay_minutes_target").alias(
        "delay_maximo"
    ),
    avg("delay_minutes_target").alias(
        "delay_promedio"
    ),
    spark_min("weather_age_minutes").alias(
        "weather_age_minimo"
    ),
    spark_max("weather_age_minutes").alias(
        "weather_age_maximo"
    )
).show()

# Variables meteorológicas principales
print("\nRANGOS METEOROLOGICOS")

df.select(
    spark_min("temperature").alias("temp_min"),
    spark_max("temperature").alias("temp_max"),
    spark_min("visibility").alias("vis_min"),
    spark_max("visibility").alias("vis_max"),
    spark_min("wind_speed").alias("wind_min"),
    spark_max("wind_speed").alias("wind_max")
).show()

# Distribución del objetivo
print("\nDISTRIBUCION DEL OBJETIVO")

df.groupBy("delay_target").count().orderBy(
    "delay_target"
).show()

demorados = df.filter(col("delay_target") == 1).count()
tasa_demora = (demorados / total) * 100

print("Vuelos demorados >= 15 minutos:", demorados)
print("Tasa de demora:", round(tasa_demora, 2))

# Guardar resumen local en salida estándar
resultado = {
    "total_registros": total,
    "total_columnas": len(df.columns),
    "vuelos_con_clima": con_clima,
    "porcentaje_con_clima": round(porcentaje_clima, 2),
    "duplicados": duplicados,
    "vuelos_demorados_15": demorados,
    "tasa_demora_15": round(tasa_demora, 2),
    "completitud": completitud
}

print("\nRESULTADO_JSON")
print(json.dumps(resultado, indent=2))

spark.stop()
