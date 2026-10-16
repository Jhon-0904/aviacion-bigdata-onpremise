# Fichas de procedencia de datos

Proyecto UPAO 2026-20 · Ruta A on-premise · Corte documentado: 16 de octubre de 2026.

Estas fichas describen **los archivos que constan en HDFS y los conteos del pipeline**. La fecha de carga en HDFS no demuestra por sí sola la fecha exacta de descarga. Antes de entregar el informe, el autor debe completar los campos marcados «pendiente» con el historial de descarga o el comprobante correspondiente.

## Fuente 1. Vuelos BTS

| Campo exigido por la guía | Registro y evidencia |
|---|---|
| Nombre del conjunto | Reporting Carrier On-Time Performance (1987–present), meses enero–julio de 2026. |
| Origen | Fuente pública institucional. |
| Entidad publicadora | U.S. Department of Transportation, Bureau of Transportation Statistics (BTS), TranStats. |
| Enlace de consulta y descarga | https://transtats.bts.gov/DL_SelectFields.aspx?QO_fu146_anzr=&gnoyr_VQ=FGJ |
| Fecha de obtención | **Pendiente de confirmar con el comprobante de descarga.** La carga en HDFS figura el 15 de octubre de 2026 a las 19:14 UTC. |
| Licencia o condiciones de uso | Acceso público desde TranStats. **Pendiente de conservar/enlazar las condiciones concretas aplicables a esta descarga**; no se atribuye una licencia específica sin evidencia. Se cita la entidad publicadora. |
| Período cubierto | Enero–julio de 2026. |
| Volumen | 7 archivos CSV mensuales, 4 120 343 filas leídas por Spark, 110 columnas de origen, 1.7 GiB informados por `hdfs dfs -du -h`. |
| Ubicación | `hdfs:///proyecto_latam/raw/bts/`. Muestra reducida: `datos/sample/bts_2026_01_01_muestra.csv`. |
| Evidencia | `outputs/evidencias/carga_fuentes_hdfs.txt` y `outputs/evidencias/01_inspeccion_fuentes_spark.txt`. |

La tabla institucional describe vuelos domésticos sin escala reportados por las aerolíneas participantes de Estados Unidos. La procedencia **no acredita operaciones de LATAM Airlines**; el uso del nombre LATAM corresponde al caso de decisión planteado, cuya aplicabilidad real requiere datos de esa empresa y autorización. La publicación institucional y sus campos se documentan en [perfil BTS](https://transtats.bts.gov/TableInfo.asp?QO_fu146_anzr=b0-gvzr&V0s1_b0yB=D&gnoyr_VQ=FGJ) y [selector de campos](https://transtats.bts.gov/DL_SelectFields.aspx?QO_fu146_anzr=&gnoyr_VQ=FGJ).

### Diccionario de campos BTS relevantes

| Campo | Significado en este proyecto |
|---|---|
| `FlightDate` | Fecha del vuelo. |
| `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Tail_Number` | Aerolínea reportante, número de vuelo y matrícula declarada. |
| `Origin`, `Dest` | Aeropuertos de origen y destino. |
| `CRSDepTime`, `CRSArrTime` | Horarios programados locales de salida y llegada; requieren fecha y zona horaria para convertir a UTC. |
| `Distance`, `DistanceGroup` | Distancia y su agrupación reportadas por BTS. |
| `Cancelled`, `Diverted` | Indicadores de cancelación y desvío presentes en la fuente cruda. |
| `DepDelayMinutes`, `DepDel15` | Demora de salida y resultado binario de al menos 15 minutos. El resultado solo se usa como objetivo o para históricos temporalmente válidos. |

## Fuente 2. Observaciones NOAA GHCNh

| Campo exigido por la guía | Registro y evidencia |
|---|---|
| Nombre del conjunto | Global Historical Climatology Network hourly (GHCNh), archivos por estación y año 2026. |
| Origen | Fuente pública institucional. |
| Entidad publicadora | National Oceanic and Atmospheric Administration (NOAA), National Centers for Environmental Information (NCEI). |
| Enlace de consulta y acceso | https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly ; documentación: https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh_DOCUMENTATION.pdf |
| Fecha de obtención | **Pendiente de confirmar con el comprobante de descarga.** La carga en HDFS figura el 15 de octubre de 2026 a las 19:14 UTC. |
| Licencia o condiciones de uso | Acceso público institucional. **Confirmar y adjuntar la ficha de derechos específica de GHCNh/archivo descargado** antes de declarar una licencia concreta. |
| Período utilizado | Del 1 de enero al 31 de julio de 2026 según mínimos y máximos por estación en `outputs/evidencias/02_estaciones_noaa.txt`. |
| Volumen | 9 archivos PSV de estación, 468 461 filas leídas por Spark, 329 columnas crudas, 210.6 MiB informados por `hdfs dfs -du -h`. |
| Ubicación | `hdfs:///proyecto_latam/raw/noaa/`. Muestras por estación: `datos/sample/GHCNh_*.psv`. |
| Evidencia | `outputs/evidencias/carga_fuentes_hdfs.txt`, `outputs/evidencias/01_inspeccion_fuentes_spark.txt` y `outputs/evidencias/02_estaciones_noaa.txt`. |

NCEI describe GHCNh como registros horarios/sinópticos de estaciones terrestres y ofrece descarga masiva PSV o Parquet. El mapeo `datos/sample/mapeo_aeropuerto_estacion.csv` vincula los nueve aeropuertos de origen con estación y zona horaria. Se unen observaciones anteriores al instante de alerta; **el archivo no acredita la hora en que NOAA publicó cada observación**, por lo que la disponibilidad operacional a T−2 horas no está demostrada.

### Diccionario de campos NOAA relevantes

| Campo | Significado en el procesamiento |
|---|---|
| `STATION` | Identificador de estación meteorológica. |
| `DATE` | Marca temporal de observación que el módulo 08 normaliza a UTC. |
| `temperature`, `dew_point_temperature`, `relative_humidity` | Temperatura, punto de rocío y humedad relativa. |
| `wind_direction`, `wind_speed`, `wind_gust` | Dirección, velocidad y ráfagas del viento. |
| `visibility`, `station_level_pressure`, `sea_level_pressure`, `precipitation` | Visibilidad, presión de estación, presión al nivel del mar y precipitación. |

Las unidades y códigos de calidad deben verificarse contra la documentación GHCNh al interpretar magnitudes; el pipeline usa conversión numérica tolerante a valores no válidos. No convertir un valor faltante en cero meteorológico sin justificarlo.

## Integración y trazabilidad

El módulo `04_preparar_bts.py` asigna estación y zona horaria a cada vuelo según `Origin`. El módulo `08_integrar_noaa.py` une cada vuelo con una observación de su estación cuyo tiempo no excede `alert_time_utc` y conserva una fila por vuelo. La salida es `hdfs:///proyecto_latam/processed/integrado`, 1 149 291 filas y 83 columnas. Los controles están en `outputs/evidencias/10_control_calidad.txt`.

**Verificación pendiente antes de entregar:** adjuntar la fecha real y URL de los archivos descargados y la evidencia de sus condiciones de uso. Los tamaños y conteos anteriores describen este lote, no el universo institucional completo.
