# Auditoría técnica de la Ruta A · 16 de octubre de 2026

## Evidencia disponible

- Los módulos 04–08 leen BTS y NOAA desde HDFS, transforman con Spark y escriben Parquet en HDFS. El lanzamiento documentado usa YARN y JupyterLab puede consultar el resultado.
- El dataset integrado registrado tiene 1 149 291 filas y 83 columnas. El repositorio contiene una muestra de los dos orígenes y el mapeo de estaciones.
- El script 13 reporta 1×, 10× y 100×, pero replica *el Parquet final* mediante `unionByName` y ejecuta `count` y un agregado. Por ello sus tiempos no representan el pipeline completo que pide la sección 3.7. `resource.getrusage(RUSAGE_SELF)` corresponde al proceso Python del controlador, no a la memoria de los ejecutores YARN.
- El script 15 entrena con enero–junio y compara alternativas y umbrales en julio. La métrica de julio se usa también para seleccionar; por ello no es una prueba independiente del modelo elegido.

## Disponibilidad temporal de variables

La alerta es dos horas antes de la salida programada. En 05, la condición `previous_flight_scheduled_arrival <= alert_time_utc` comprueba una *llegada programada*, pero no verifica que la demora final del vuelo anterior ya estuviera registrada. Los promedios en 05–07 usan resultados de vuelos anteriores por orden de **salida programada**; algunos aún podrían estar en curso cuando se emite la alerta. Estos campos no deben interpretarse como predictores libres de fuga temporal hasta reconstruir la hora de disponibilidad de cada resultado.

En 06, `airport_active_flights_same_hour` agrupa por aeropuerto, año, mes y hora, sin día; por tanto mezcla días del mes. Las columnas `airport_previous_cancelled_count` y `airport_previous_diverted_count` son nulas. `airport_arrivals_previous_2h` no aparece. La unión NOAA utiliza observaciones fechadas antes de la alerta, pero el conjunto carece de hora de publicación: la disponibilidad real de NOAA en ese instante sigue siendo un supuesto.

## Corrección ejecutable de evaluación

`scripts/16_evaluacion_temporal_segura.py` usa únicamente programación BTS y tiempo de observación NOAA anterior a la alerta. Entrena en enero–mayo, compara tres modelos y cuatro umbrales en junio, reentrena el candidato escogido con enero–junio y evalúa julio **una vez**. Escribe salidas nuevas en HDFS, sin modificar el modelo 15 ni el Parquet. El resultado medido deberá presentarse como prueba del prototipo con variables restringidas, sin prometer una mejora numérica. La falta de una hora de publicación NOAA debe constar como limitación.

## Trabajo que aún exige la guía

1. Sección 3.6: ejecutar la misma carga antes y después de una optimización y registrar tiempos comparables y recursos.
2. Sección 3.7: ejecutar ingesta, limpieza, integración y escritura con réplicas 10× y 100× en rutas separadas. La VM reportó 12 GB de HDFS disponibles; dimensionar y vigilar espacio antes de lanzar 100×. Si falla por capacidad, registrar el umbral real y no presentar el tiempo del script 13 como sustituto.
3. Sección 4.4: tabla de hechos, dimensiones, granularidad y tablero de 3–5 indicadores con fórmula, frecuencia y responsable.
4. Admisibilidad y secciones 0–6: fichas de procedencia con enlaces/licencias/fechas, declaración de IA firmada, evidencia de corrida completa, línea base de negocio real y alcance individual autorizado por el docente. El origen BTS de vuelos estadounidenses no demuestra operaciones de LATAM.

No se ha ejecutado aquí el código nuevo en la VM; registrar métricas únicamente tras la corrida efectiva sobre YARN.
