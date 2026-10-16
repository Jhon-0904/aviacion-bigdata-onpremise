# Capa de consumo descriptiva (sección 4.4)

Ejecutar `scripts/17_capa_consumo.py` con Spark sobre YARN. La tabla de hechos `fact_vuelo` tiene **una fila por vuelo programado** del conjunto integrado. Su `flight_key` es un hash técnico de fecha, aerolínea, número, matrícula, origen, destino y hora programada; la matrícula no se expone como columna del hecho. Las dimensiones `dim_fecha`, `dim_aeropuerto` y `dim_aerolinea` conectan mediante `fecha`, `origin_code`/`dest_code` y `airline_code`. Todos los Parquet se escriben en `/proyecto_latam/consumo`.

El indicador agregado `kpi_aeropuerto_mes` contiene los siguientes valores para cada aeropuerto de origen y mes. Su frecuencia de actualización propuesta es mensual, tras ingresar datos BTS/NOAA nuevos y ejecutar controles de calidad. **Responsable propuesto para un piloto:** analista de operaciones; el proyecto no acredita adopción por LATAM.

| Indicador | Fórmula | Decisión propuesta |
|---|---|---|
| Vuelos | Conteo de filas de `fact_vuelo` | Priorizar aeropuertos con más carga de análisis |
| Tasa de demora ≥15 min | `sum(delayed_15)/count(*)` | Revisar evolución de puntualidad por aeropuerto |
| Demora media (min) | `avg(delay_minutes)` sobre vuelos con valor | Inspeccionar intensidad de las demoras |
| Cobertura meteorológica | `avg(weather_match)` | Solicitar revisión de capturas NOAA cuando baje la cobertura |

Estos indicadores describen vuelos BTS de Estados Unidos. No son métricas de LATAM. El tablero debe leer `kpi_aeropuerto_mes`; los resultados posteriores a la salida son válidos para análisis retrospectivo, aunque no deben incorporarse como predictores de una alerta T−2h.
