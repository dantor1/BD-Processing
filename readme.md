# Big Data Processing **Spark-Scala**

Proyecto enfocado para construir mediante una arquitectura lambda, los datos de una antena de telefonía. Para ello se realiza de forma resumida los siguientes pasos:

* Se levanta una máquina virtual en Google Cloud
* Dentro de ella utilizaremos la base de datos PostgreSQL para almacenar los datos.
* También será necesario Docker, en el cual se instalará una imagen para el envío de datos.
* Instalaremos Apache Kafka para que en tiempo real se reciban los datos, esto se realizará mediante los topics denominados devices.

Se ha realizado mediante 3 carpetas:

1. Batch Layer (batch): Capa de procesamiento por lotes se encarga de computar resultados usando grande cantidades de datos, alta latencia.

2. Serving Layer (provisioner): Capa encargada de servir los datos, es nutrida por las dos capas anteriores.

3. Speed Layer (streaming): Capa de procesamiento en streaming, el cual computa resultados en tiempo real y baja latencia.