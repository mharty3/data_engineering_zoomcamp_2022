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

## Installing Spark on Linux
[Video](https://www.youtube.com/watch?v=hqUbB9c8sKg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=49)
[Instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md)

* Install Java Open JDK 11. Spark requires version 8 or 11. Note, this is not the most recent version of Java!
  
  ```bash
  mkdir spark
  cd spark
  wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
  tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
  rm openjdk-11.0.2_linux-x64_bin.tar.gz
  export JAVA_HOME="$HOME/spark/jdk-11.0.2"
  export PATH="$JAVA_HOME/bin:$PATH"
  ```

* Install Spark

  ```bash
     wget https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz
     tar xzfv spark-3.0.3-bin-hadoop3.2.tgz
     rm spark-3.0.3-bin-hadoop3.2.tgz
     export SPARK_HOME="$HOME/spark/spark-3.0.3-bin-hadoop3.2"
     export PATH="$SPARK_HOME/bin:$PATH"
  ```

* Run `spark-shell.`  There are some warnings, but we can ignore them. This will open a scala prompt.

* add the following to .bashrc

  ```bash
  export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
  export PATH="${JAVA_HOME}/bin:${PATH}"
  export SPARK_HOME="${HOME}/spark/spark-3.0.3-bin-hadoop3.2"
  export PATH="${SPARK_HOME}/bin:${PATH}"
  ```

## Using Pyspark
[Instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md)
  * Update the Python Path for Pyspark

  ```bash
  export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
  export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
  ```

* create a directory for the example, download a csv for testing, and run jupyter notebook. Use vs code to forward port 8080 from the VM.
  ```bash
  mkdir spark_example
  cd spark_example
  wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
  jupyter notebook
  ```

* Test out pyspark in the notebook

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()
```

Also test writing files

```python
df.write.parquet('zones')
```

* Also forward port 4040 using vs code to see the Spark Jobs that have been run