# Aviación Big Data On-Premise

Proyecto académico de Big Data y Analítica de Datos — UPAO, periodo 2026-20.

**Ruta de despliegue: A · On-premise.** HDFS almacena los datos, YARN gestiona los recursos, Apache Spark ejecuta el procesamiento y el modelado, y JupyterLab se utiliza para el desarrollo y análisis. Los componentes se ejecutan en una máquina virtual Ubuntu local, sin servicios cloud.

## Propósito y alcance

El proyecto integra registros de vuelos del Bureau of Transportation Statistics (BTS) con observaciones meteorológicas NOAA GHCNh. A partir de estas fuentes construye variables operativas y clasifica vuelos según su riesgo de demora de salida de al menos 15 minutos.

La variable objetivo del dataset es `delay_target`:

- `1`: demora de salida de 15 minutos o más.
- `0`: demora de salida inferior a 15 minutos.

El resultado es un **prototipo académico de priorización para revisión humana**. No toma decisiones automáticas ni demuestra que pueda evitar demoras.

**Limitación fundamental:** BTS contiene vuelos de aerolíneas estadounidenses en aeropuertos de Estados Unidos. Los resultados no representan operaciones reales de LATAM ni constituyen una medición del desempeño de LATAM.

## Autor

|Jhon Maximiliano Villanueva|000274195| Desarrollo individual|

## Requisitos del entorno

Entorno utilizado en la ejecución documentada:

| Componente | Versión o configuración |
|---|---|
| Máquina virtual | Ubuntu 26.04.1 LTS, ARM64 |
| Java | OpenJDK 17 |
| Hadoop y HDFS | 3.4.2 |
| YARN | Incluido en Hadoop 3.4.2 |
| Apache Spark | 4.0.4, distribución para Hadoop 3 |
| Python | Python disponible en Ubuntu; JupyterLab en entorno virtual |
| JupyterLab | Instalación en `~/venv-jupyter` |

Rutas de la instalación utilizada:

```text
HADOOP_HOME=/opt/hadoop
SPARK_HOME=/opt/spark-4.0.4-bin-hadoop3
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
```

Estas rutas son propias de la máquina virtual del proyecto. Si se reproduce en otra máquina, deben ajustarse a la instalación local.

## Fuentes y ubicación de los datos

| Fuente institucional | Periodo utilizado | Archivos | Registros u observaciones |
|---|---|---:|---:|
| BTS, On-Time Reporting Carrier On-Time Performance | Enero–julio de 2026 | 7 CSV mensuales | 4 120 343 vuelos |
| NOAA NCEI, GHCNh | Enero–julio de 2026 | 9 PSV de estaciones | 468 461 observaciones filtradas |

Fuentes institucionales:

- BTS: https://www.transtats.bts.gov/
- NOAA GHCNh: https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-hourly

Los archivos completos no están en GitHub por su tamaño. Para reproducir el procesamiento se deben obtener los mismos archivos institucionales y el mismo periodo, conservarlos sin modificaciones en la zona `raw` y documentar sus fechas de descarga y condiciones de uso en las fichas de procedencia.

Rutas esperadas en HDFS:

```text
/proyecto_latam/raw/bts/       7 archivos CSV de BTS
/proyecto_latam/raw/noaa/      9 archivos PSV de NOAA filtrados a enero–julio
/proyecto_latam/reference/mapeo_aeropuerto_estacion.csv
```

El archivo de referencia relaciona los aeropuertos `DEN`, `DFW`, `MCO`, `ATL`, `CLT`, `LAS`, `LAX`, `PHX` y `SEA` con nueve estaciones NOAA y sus zonas horarias. **Ese archivo también es necesario para reproducir el pipeline**; compruebe que está incluido en el repositorio antes de presentar la entrega.

Las fichas de procedencia deben registrar, por fuente: entidad publicadora, URL exacta de descarga, fecha de obtención, periodo, formato, tamaño, cantidad de registros y licencia o condiciones de uso.

## Organización del repositorio

```text
README.md
scripts/                 Procesamiento, controles, EDA y modelado
notebooks/               Verificación y análisis en JupyterLab
docs/                    Arquitectura, procedencia y documentación
outputs/figures/         Figuras exportadas
outputs/evidencias/      Registros y capturas de las ejecuciones
```

Los datos crudos, los Parquet completos, los modelos guardados en HDFS, los entornos virtuales y las credenciales no deben incorporarse al repositorio.

## Organización del data lake

```text
/proyecto_latam/
├── raw/
│   ├── bts/
│   └── noaa/
├── reference/
│   └── mapeo_aeropuerto_estacion.csv
├── staging/
│   ├── bts_preparado/
│   ├── bts_prop/
│   ├── bts_congestion/
│   └── bts_operativo/
├── processed/
│   └── integrado/
└── outputs/
    ├── eda/
    ├── metricas/
    └── modelos/
```

Las zonas `raw`, `staging` y `processed` separan los archivos de origen, las transformaciones intermedias y el dataset analítico. Los resultados se escriben en `outputs`.

## Preparación para reproducir

### 1. Instalar y configurar la Ruta A

Instale Java 17, Hadoop 3.4.2, Spark 4.0.4, Python y JupyterLab. Configure Hadoop en modo de un nodo con HDFS y YARN y compruebe que Spark puede enviar trabajos a YARN. La configuración de rutas, memoria y directorios locales debe adaptarse a la máquina de destino.

**No ejecute `hdfs namenode -format` sobre una instalación que ya contenga datos:** esa operación puede inutilizar el sistema de archivos existente.

### 2. Iniciar los servicios

```bash
start-dfs.sh
start-yarn.sh
jps
yarn node -list
hdfs dfsadmin -report
```

En la ejecución de referencia se observan `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager` y `NodeManager`, además de un nodo YARN en estado `RUNNING`.

### 3. Cargar las fuentes en HDFS

Cree, si aún no existen, las rutas de entrada:

```bash
hdfs dfs -mkdir -p /proyecto_latam/raw/bts
hdfs dfs -mkdir -p /proyecto_latam/raw/noaa
hdfs dfs -mkdir -p /proyecto_latam/reference
```

Cargue los siete CSV de BTS, los nueve PSV NOAA **ya filtrados al periodo enero–julio de 2026** y el CSV de mapeo en las rutas indicadas. Use rutas locales que correspondan a la ubicación real de las descargas en su máquina.

Antes de procesar, verifique:

```bash
hdfs dfs -ls /proyecto_latam/raw/bts
hdfs dfs -ls /proyecto_latam/raw/noaa
hdfs dfs -cat /proyecto_latam/reference/mapeo_aeropuerto_estacion.csv
```

Los conteos de referencia son siete archivos BTS, nueve archivos NOAA y nueve correspondencias aeropuerto–estación.

### 4. Ejecutar el pipeline

Desde la raíz del repositorio, ejecute los scripts existentes de `scripts/` en este orden:

```text
01_inspeccionar_fuentes.py
02_identificar_estaciones_noaa.py
03_validar_mapeo_bts.py
04_preparar_bts.py
05_variables_propagacion.py
06_variables_congestion.py
07_historial_aerolinea_ruta.py
08_integrar_noaa.py
10_control_calidad.py
11_eda_modelo_base.py
12_generar_figuras.py
13_escalabilidad_pipeline.py
15_modelo_definitivo.py
```

Los trabajos Spark se lanzan con:

```bash
spark-submit --master yarn --deploy-mode client scripts/NOMBRE_DEL_SCRIPT.py
```

Sustituya `NOMBRE_DEL_SCRIPT.py` por cada script Spark en el orden anterior. `12_generar_figuras.py` se ejecuta con Python y requiere las tablas de EDA exportadas a las rutas locales que utiliza ese script. Revise las rutas configuradas dentro de cada archivo antes de ejecutar una reproducción en una máquina diferente.

No se debe ejecutar el modelado antes de comprobar que la escritura del dataset integrado y el control de calidad terminaron correctamente.

### 5. Verificar el dataset procesado

```bash
hdfs dfs -ls /proyecto_latam/processed/integrado
hdfs dfs -du -h /proyecto_latam/processed/integrado
```

Resultados observados en la corrida documentada:

```text
1 149 291 vuelos integrados
83 columnas
Formato Parquet
Partición scheduled_year=2026
0 duplicados por la clave de vuelo utilizada en el control de calidad
```

La correspondencia meteorológica reportada fue del 100 % para los vuelos conservados tras restringir el análisis a los nueve aeropuertos mapeados. Esto **no significa** que el 100 % de todos los vuelos BTS originales tenga cobertura NOAA.

## JupyterLab

En la máquina virtual, active el entorno y abra JupyterLab:

```bash
source ~/venv-jupyter/bin/activate
cd ~/aviacion_bigdata_onpremise/notebooks
jupyter lab --ip=127.0.0.1 --port=8888 --no-browser
```

Abra en el navegador la URL con token que aparece en la terminal. No publique el token de JupyterLab ni capturas que lo muestren.

Notebooks del proyecto:

```text
01_verificacion_ruta_onpremise.ipynb
02_analisis_modelo_latam.ipynb
```

Seleccione el kernel `Python - Aviacion On-Premise`. La primera notebook verifica Spark 4.0.4 sobre YARN y la lectura del Parquet desde HDFS; la segunda consulta el dataset y las métricas del modelo.

Si se reinició la VM, inicie HDFS y YARN **antes** de crear la sesión Spark en JupyterLab.

## Ingeniería de variables

El dataset combina:

- Planificación del vuelo: aeropuerto, ruta, aerolínea, matrícula, hora, calendario y distancia.
- Propagación: información histórica de la misma aeronave.
- Congestión: actividad programada e historial del aeropuerto.
- Historial: comportamiento anterior de aerolínea, ruta y combinaciones aeropuerto–aerolínea.
- Meteorología: observaciones NOAA asociadas al aeropuerto y anteriores a la alerta.

La alerta se define dos horas antes de la salida programada (`alert_time_utc`). La validez temporal de cada variable debe comprobarse con respecto a ese instante. En particular, una demora final de otro vuelo no puede usarse como predictor si aún no se conocía al momento de la alerta.

Las columnas no disponibles no deben reemplazarse por valores inventados. Las variables de cancelaciones, desviaciones y llegadas previas que no hayan quedado implementadas de forma verificable se documentan como limitación y no se declaran como predictores efectivos.

## Análisis y modelos

El análisis exploratorio se realizó sobre el dataset limpio e incluye figuras por aeropuerto, hora y visibilidad en `outputs/figures/`.

El modelo base fue una regresión logística. En la corrida documentada obtuvo:

| Métrica de la clase positiva | Modelo base |
|---|---:|
| Precisión | 0.6402 |
| Recall | 0.2096 |
| F1 | 0.3158 |
| Falsos positivos | 6 621 |
| Falsos negativos | 44 438 |

Se compararon regresión logística, Random Forest y GBTClassifier con separación temporal: enero–junio de 2026 para entrenamiento y julio de 2026 para prueba. Según el criterio utilizado por el script, se guardó un GBTClassifier con umbral de alerta de 0.50:

| Métrica de la clase positiva | GBT, umbral 0.50 |
|---|---:|
| Precisión | 0.5057 |
| Recall | 0.5457 |
| F1 | 0.5249 |
| ROC-AUC | 0.7083 |
| PR-AUC | 0.5112 |
| Falsos positivos | 29 984 |
| Falsos negativos | 25 544 |
| Alertas por cada 1 000 vuelos | 345.84 |

Métricas completas:

```text
/proyecto_latam/outputs/metricas/modelo_definitivo
```

Modelo guardado:

```text
/proyecto_latam/outputs/modelos/modelo_definitivo
```

**Interpretación:** aumentó el recall y el F1 positivo frente al modelo base, pero también aumentaron las alertas y los falsos positivos. No debe afirmarse que reduce falsas alertas. La selección del modelo y del umbral requiere una validación temporal independiente adicional antes de considerarlo listo para producción; comparar alternativas y escoger usando el mismo mes de prueba limita la solidez de la evaluación final.

## Prueba de escalabilidad

La corrida documentada replicó el **dataset procesado** exclusivamente para medir rendimiento:

| Factor | Filas procesadas | Tiempo registrado |
|---|---:|---:|
| 1× | 1 149 291 | 1.61 s |
| 10× | 11 492 910 | 6.35 s |
| 100× | 114 929 100 | 34.67 s |

Las réplicas no alimentaron el EDA, el entrenamiento ni las conclusiones de negocio.

**Alcance de la evidencia:** estos tiempos corresponden a la prueba implementada sobre datos procesados. No equivalen a ejecutar nuevamente **todo** el pipeline, desde la ingesta y limpieza de BTS/NOAA hasta la escritura final, sobre 10× y 100×. La prueba de pipeline completo solicitada en la sección 3.7 de la guía sigue pendiente y debe identificarse así en el informe.

## Evidencias y documentación

- `outputs/evidencias/`: registros de ejecuciones y comprobaciones.
- `outputs/figures/`: figuras del análisis exploratorio.
- `notebooks/`: verificación de la Ruta A y análisis.
- `docs/`: fichas de procedencia, arquitectura y documentación, según los archivos efectivamente publicados.

La existencia de un archivo en el repositorio debe verificarse antes de citarlo como evidencia en el informe.

## Datos de muestra y reproducción desde cero

La guía exige **datos de muestra si el conjunto completo no puede publicarse**. Al no incluirse los datos originales completos, se debe agregar a `datos/sample/` una muestra pequeña de BTS y NOAA, con sus encabezados reales, junto con el mapeo aeropuerto–estación necesario para ejecutarla.

**Estado:** confirme que esos archivos de muestra estén publicados antes de considerar cumplido este requisito. Este README documenta el procedimiento de la corrida completa, pero por sí solo no reemplaza archivos de entrada ausentes ni una instalación automatizada del entorno.

## Fuentes consideradas y no integradas

FAA, NOTAM, TAF y OpenSky se consideraron como posibles ampliaciones. No forman parte del dataset ni de las métricas reportadas. Su integración requeriría archivos reales, cobertura temporal comprobada, claves de unión verificables y evidencia de disponibilidad antes de la alerta.

## Privacidad y seguridad

Los archivos originales permanecen fuera de GitHub. No publique tokens de GitHub, tokens de JupyterLab, contraseñas ni datos personales en notebooks, registros o capturas.

El entorno descrito es académico y de un solo nodo. Su configuración local no debe presentarse como una plataforma de alta disponibilidad ni como un despliegue productivo con cifrado y controles avanzados ya implementados.

La declaración formal de uso de IA generativa, revisada y firmada conforme al Anexo A de la guía, debe incluirse en el informe. Una mención en este README no la sustituye.

## Repositorio

https://github.com/Jhon-0904/aviacion-bigdata-onpremise

El historial de commits registra los cambios publicados. Las instrucciones de este README describen la ejecución conocida; los requisitos de procedencia, muestras, pipeline completo de escalabilidad y documentación del informe deben verificarse por separado antes de la entrega.
