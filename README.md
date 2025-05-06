# Big Data y Transporte Público en Santiago de Chile

Este proyecto implementa una arquitectura de Big Data en Google Cloud Platform (GCP) para analizar datos del transporte público en Santiago, utilizando procesos batch y en tiempo real.

## Estructura del proyecto

- `dataflow_pipeline/`: scripts Python y configuraciones para ejecutar pipelines de limpieza y carga de datos.
- `pubsub_ingestion/`: script de ingesta en tiempo real desde API pública a Pub/Sub y Cloud Storage.
- `docs/`: PDF resumen y documentos de apoyo.
- `images/`: capturas del dashboard y visualizaciones en Looker Studio.

## Tecnologías
- Google Cloud Storage
- BigQuery
- Dataflow (Apache Beam)
- Pub/Sub
- Looker Studio
- Python 3.8

## Resultados
- Predicciones en tiempo real
- Reportes dinámicos
- Análisis de accesibilidad, rutas y servicios por empresa

## Documentos
- [PDF Resumen del Proyecto](docs/resumen_proyecto_bigdata.pdf)
- [PDF Detallado del Proyecto](https://github.com/Nadarellano/bigdata-transporte-santiago/blob/main/docs/Informe_Completo_Proyecto_Transporte_BigData_GCP.pdf)
- [Ver presentación en LinkedIn](https://www.linkedin.com/in/nadia-aracelly-arellano-gonz%C3%A1lez-426aa721)
