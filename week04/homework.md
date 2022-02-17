## Week 4 Homework - WIP
[Form](https://forms.gle/B5CXshja3MRbscVG8) 

We will use all the knowledge learned in this week. Please answer your questions via form above.  
* You can submit your homework multiple times. In this case, only the last submission will be used. 
**Deadline** for the homework is 21th Feb 2022 17:00 CET.


In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:
* Building a source table: stg_fhv_tripdata
* Building a fact table: fact_fhv_trips
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1: 
**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)**  
You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

```SQL
SELECT COUNT(1) count
FROM `data-eng-zoomcamp-339102.dbt_mharty.fact_trips` 
WHERE EXTRACT(YEAR FROM pickup_datetime) IN  (2019, 2020)
```

61,587,378

### Question 2: 
**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase.
I didn't use a dashboard, I used a query for this part.

```SQL
SELECT service_type, COUNT(1) count
FROM `data-eng-zoomcamp-339102.dbt_mharty.fact_trips` 
WHERE EXTRACT(YEAR FROM pickup_datetime) IN  (2019, 2020)
GROUP BY service_type
```
Green: 6255716
Yellow: 55331662

This comes out to about 10.1% green and 89.9% yellow.


### Question 3: 
**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

```SQL
SELECT COUNT(1) 
FROM `data-eng-zoomcamp-339102.dbt_mharty.stg_fhv_tripdata` 
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```

42,084,899


### Question 4: 
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

```SQL
SELECT COUNT(1) 
FROM `data-eng-zoomcamp-339102.dbt_mharty.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```

22,676,253

### Question 5: 
**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

```SQL
SELECT 

  EXTRACT(MONTH FROM pickup_datetime) as month,
  COUNT(1) as cnt

FROM `data-eng-zoomcamp-339102.dbt_mharty.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
GROUP BY month
ORDER BY cnt DESC 
```

January has much more than any other month. Is this a data error? Or could it be NYE partiers? :O