# Week 3: Data Warehouse

[Slides](https://www.linkedin.com/posts/michaelharty3_github-mharty3dataengineeringzoomcamp-activity-6893745899370102784-JsN0)

[Video](https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25)

* A data warehouse is a system for housing queryable datasets from heterogeneous sources in a single repository.

## Big Query

* serverless data warehouse - no servers to configure. You don't have to build and maintain the infrastructure. Google does it for you.
* can scale easily from gigabytes and petabytes
* Has built in features like
  * machine learning
  * geospatial
  * business intelligence
* BQ separates compute engine and storage. Beneficial in terms of cost
* Considerations for BQ in the class
    * BQ Caches data. for our demo, we want to disable that. (In more, query settings)
    * BQ provides a lot of open data. earch for the project eg. nyc citibike_stations. This is a small dataset
    * you can run SQL in the editor, and export the results or visualize on a dashboard in google data studio
* BQ Cost
  * On demand $5 for 1TB processed on demand
  * Flat price model can make sense for over 200TB but your compute at any given time is limited to the slots you have purchased
* You can create an *external table* from csv files stored in google cloud storage, and the data is not stored in BQ.

## Clustering and Partitioning
[Video](https://www.youtube.com/watch?v=-CqXf7vhhDs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=26)
* Create a **partitioned table** by partitioning on a data field. BQ will group data together that has the same value for that field. This way, when processing data it will not have to look into partitions it doesn't have to. This will save a lot of processing cost
* Data within partitions can be **clustered** on a second field. This has a simlar effect of saving cost.
* Determining which columns to partition and cluster upon will depend on how you plan to query the data. If you will often be querying by date, partition by date
* You can partition based on ingestion time: daily, hourly, monthly, or yearly. BQ has limit of 4000 partitions
* Table with small data <1GB can be more expensive to partition and cluster than not
* In clustering, cost benefit is not known before running the query. In partitioning you can set restrictions to not run a query if it will exceed a certain amount of processing
* Clustering provides more granularity
* Do not use partitioning on columns with high cardinality or on columns that will change frequently and cause the partitions to be frequently recalculated
* **Automatic reclustering** as more data is inserted into the table, BQ will automatically re-cluster the table in the background with no cost

## BQ Best practices to reduce cost and improve performance
[Video](https://www.youtube.com/watch?v=k81mLJVX08w&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=27)
### Cost Reduction

* Avoid `SELECT *`. Specify column names since BQ uses columnar storage. If you only need a handful of columns, specify them
* Price queries before running them. Price can be seen in the top right
* Use clusters or partitioned tables
* use streaming inserts with caution. these can be very expensive
* materialize queries eg. CTEs. 

### Query Performance
* Filter on partitioned columns
* Denormalize data
* don't use external tables if high performance is critical
* reduce data before using a join
* Optimize joins by placing the largest table first, and then decreasing size of tables in terms of number of rows


## Internals of Big Query
[Video](https://www.youtube.com/watch?v=eduHi1inM4s&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=28)

* You don't need to know the internals of BQ in order to use it effectively. Just understand the above best practices, partitioning, and clustering
* BQ uses storage called Colossus, which is very cheap. Most cost is reading data and running queries. aka compute.
* The Jupiter network inside google data centers connects the compute to the storage in a 1TB/s network
* The query execution engine is Dremel. It creates query structures and divides it into parts that can be run on separate nodes that fetch data from colossus and perform aggregations

## Machine Learning in BQ
[Video](https://www.youtube.com/watch?v=B-WtpB0PuG4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=29)
[BQ ML code](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_ml.sql)

* no python or java needed for ML in BQ, all you need to know is SQL and ML concepts and algos
* Data does not need to be exported from BQ, all ML is conducted within the data warehouse
* Pricing:

  * Free: 10GB/month of storage, 1 TB/month data processed, ML create model first 10 GB per month is free
  * after that cost vary by number of TB and type of model

* In this video we will build a linear regression model based on the taxi data to predict `tip-amount`
* BQ has some built in automatic functionality for feature engineering/preprocessing like standardization and onehot encoding and also manual processing steps
* See the code linked above for an example of training a linear regression model

## ML deployment
[Video](https://www.youtube.com/watch?v=BjARzEWaznU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=30)
[instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/extract_model.md)
[gcloud tutorial](https://cloud.google.com/bigquery-ml/docs/export-model-tutorial)
* This video demonstrates how to extract the model created in the last video from BQ and serve it using the `tensorflow/serving` docker image.

# Big Query Workshop
[Video](https://www.youtube.com/watch?v=lAxAhHNeGww&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=31)
* This week in the workshop, we will extend our data pipelines created last week to move data from the data lake storage bucket into the Big Query Data Warehouse that we built in week 1 with Terraform.
* We are able to query the data we loaded into BQ during week 1 and week 2 in the google cloud console.

  ```SQL
  SELECT * FROM `data-eng-zoomcamp-339102.trips_data_all.external_table` LIMIT 10
  ```

* Now let's work on writing the dags to move data from GCS to Big Query.
* First copy the airflow directory from week02 to week03. We will be modifying those dags for our current exercise. Run `docker-compose down` for all of the containers on the VM. Then cd into week03 and run `docker-compose up` to spin up all of the services needed for this week. You can delete all the logs in the logs dir.
* Using [last week's example dag](week03/airflow/dags/data_ingestion_gcs_dag.py) as a template, create a new dag called [gcs_to_bq_dag.py](week03/airflow/dags/gcs_to_bq_dag.py). Delete any unneceesary lines relating to the parquet or csv files as we won't be needing them. 



