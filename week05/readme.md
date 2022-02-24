# Week 5: Batch Processing

## Batch vs Streaming

[Video: Introduction to Batch Processing](https://www.youtube.com/watch?v=dcHe5Fl3MF8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

* A batch job is a job that takes data for an entire time period (1 day, 1 hour etc) and runs a job to process it
* In contrast, a streaming data process processes data as events happen for example, as a taxi rider is picked up an event is created and processed in a data stream.
* Key technologies for batch jobs:
  * python, like week 1
  * SQL, like week 4
  * Spark
  * Flink
* Batch jobs can be run in Kubernetes, VM, AWS Batch etc
* Typically they are orchestrated with Airflow
* Advantages:
  * Easier to manage (jobs can be retried if failed)
  * Can scale easier
* Disadvangates:
  * Takes time to process the data. If data is processed every hour, and the process takes 20 minutes, you will have to wait 90 minutes until the data is processed from when it is created.
  * Often this is not a concern because most metrics are not that time sensitive, but it can be solved with streaming
* Most jobs in industry are batch jobs (maybe 80%)

## Intro to Apache Spark

[Video: Introduction to Spark](https://www.youtube.com/watch?v=dcHe5Fl3MF8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

* Apache Spark is an open-source unified analytics engine for large-scale data processing. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance
* Multi-language. Originally written in Scala, but Pyspark (python) is very common 
* Typically it is used when data is stored in a data lake, spark will read in parquet files, do some processing, and write it back in to the data lake
* Solutions like Hive or Presto/Athena can allow you to use SQL in the data lake. That solution is often better than Spark. When it is not possible, (in machine learning for example), use spark 