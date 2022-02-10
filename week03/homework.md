# Week 3 homework

[Homework Questions](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/homework.md)

## Data Pipeline
These questions will require moving all of the FHV data over from gcs into BQ. This will be similar to the workshop sejal did with the yellow cab data.
My dag is [here](airflow/dags/gcs_to_bq_dag.py)

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)

```SQL
SELECT count(1) 
FROM `data-eng-zoomcamp-339102.trips_data_all.fhv_tripdata_partitoned` 
WHERE EXTRACT(YEAR FROM dropoff_datetime) = 2019
```
Answer: 42,084,424 trips

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

```SQL
SELECT COUNT(DISTINCT (dispatching_base_num))
FROM `data-eng-zoomcamp-339102.trips_data_all.fhv_tripdata_partitoned`
WHERE EXTRACT(YEAR FROM dropoff_datetime) = 2019 
```
Answer: 792 dispatching base nums

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

Since we are filtering by a single column, it will be best to partition by that column (`dropoff_datetime`) and then cluster by the `dispatching_base_num`

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

```SQL
SELECT COUNT(1)
FROM `data-eng-zoomcamp-339102.trips_data_all.fhv_tripdata_partitoned`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31' AND 
dispatching_base_num IN ('B00987', 'B02060', 'B02279')
```
Answer: Count: 26,558
        Estimated Data: 400MB
        Actual Data: 135MB 

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Clustering cannot be created on all data types.

Answer: For filtering by multiple columns cluster on both. Integers and Timestamps can both be clustered. 

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

Answer: Can be worse!

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

Answer: Columnar