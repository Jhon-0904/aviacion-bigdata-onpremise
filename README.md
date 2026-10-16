# Proyecto Big Data para alerta temprana de demoras

Proyecto individual para la asignatura Big Data y Analítica de Datos de la UPAO.

## Ruta de despliegue

Se utiliza la Ruta A — On-premise:

- Ubuntu en máquina virtual local.
- HDFS para almacenamiento distribuido.
- YARN para gestión de recursos.
- Apache Spark 4.0.4 para procesamiento.
- Spark MLlib para modelado.
- JupyterLab como entorno de desarrollo.

## Fuentes de datos

- BTS On-Time Reporting.
- NOAA GHCNh.

Los datos completos no se incluyen en GitHub debido a su tamaño. El procedimiento de obtención y carga en HDFS se documentará en el informe.

## Estructura

- `scripts/`: scripts de procesamiento y modelado.
- `notebooks/`: notebooks de JupyterLab.
- `docs/`: documentación y arquitectura.
- `outputs/figures/`: gráficos generados.
- `outputs/evidencias/`: evidencias de ejecución.

## Autor

Maximiliano Villanueva Jhon
