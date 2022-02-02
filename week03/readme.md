# Week 3: Data Warehouse

[slides](https://www.linkedin.com/posts/michaelharty3_github-mharty3dataengineeringzoomcamp-activity-6893745899370102784-JsN0)

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

