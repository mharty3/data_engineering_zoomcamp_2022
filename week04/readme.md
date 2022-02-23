# Week 4: Analytics Engineerng

Videos:
[Analytics Engineering Basics](https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=34)

## Analytics Engineering basics

*  Analytics engineering bridges the gap between data engineers, who are good software engineers who may not know how the data will be used, and data analysts, who are very familiar with the data but typically not as experienced at software engineering
* Analytics engineers are concerned with data modeling in the data warehouse with tools like dbt or Dataform and data presentation with tools like Looker, Mode, or Tableau

### Data Modelling Concepts

* **ETL vs ELT**

  * **ETL** - Extract transform load - Transform the data before loading it into the data warehouse
  * **ELT** - Extract load transform - Use the compute within the data warehouse to transform the data

![elt vs etl](img/etl_vs_elt.PNG)

* Kimballs Dimensional Modeling
  * Deliver data that is understandable to the end user, but also optimize query performance
  * Does not prioritize non-redundant data
  * other approaches to compare to are Bill Immon or Data vault
  * Elements:
    * Fact tables - contain metrics, facts, or measurements. correspond to a business process. Think of verbs
    * Dimensions tables - correspond to business entity, provides contect to a business process. think of nouns
    * A fact table is usually "surrounded" by many dimension tables. This is referred to as a **Star Schema**
  * Architecture:
    * Staging area - raw data, only exposed to data or analytics engineers who know what to do with it. Think of raw ingrediant storage in a restaurant
    * Processing area - this is where raw data is modeled. Think of the kitchen in a restaurant
    * Presentation area - exposed to the business users. Think of the dining area in a restaurant

### What is dbt
[Video](https://www.youtube.com/watch?v=4eCouvVOJUw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=35)
[dbt Project Repo](https://github.com/mharty3/dbt_project_DEZC_2022)
* dbt or data build tool is a tool for transforming raw data that has been loaded into a warehouse so it can be exposed to BI applications or other business data consumers
* it works by defining *models* as sql files that contain SELECT statements. dbt will compile the data and push the compute to the DW
*  **dbt Core** Open source tool that allows for data transformation. Builds and runs the dbt project, includes SQL compilation logic, and a CLI to run dbt commands locally
* **dbt CLoud** SaaS application to develop and maintain dbt projects. Free for individuals
* For our class with the Big Query data warehouse, we will develop using the dbt cloud IDE.In Big Query. For postgres local database you will need to install dbt locally and connect it to the postgres db

### Creating a dbt project from the beginning
* This workshop requires green taxi data for 2019 and 2020 which I had not uploaded to the data warehouse yet. I needed to modify the dags to do so.
    * I copy-pasted the [`yellow_taxi_ingestion_dag.py`](week03/airflow/dags/yellow_taxi_ingestion_dag.py) and created [`green_taxi_ingestion_dag.py`](week03/airflow/dags/green_taxi_ingestion_dag.py). A better practice would be to parameterize the single dag to read in fhv, yellow, and green taxi data. Maybe I will re-factor this at some point. 
    * I did refacor and parameterize the `gcs_to_bq.py` dag but ran into issues because one of the columns (`ehail_fee`) in the green taxi data contains some null values, but the parquet file treated them as ints rather than floats. I took the advice of someone on slack and simply did not upload that column to the partitioned table by using an `EXCEPT` statement. Hopefully we don't need it!
* I created an account for dbt Cloud and created service account credentials in cloud console with access to big query. I also created a new [repository](https://github.com/mharty3/dbt_project_DEZC_2022) to hold the dbt project and added a deploy key to allow dbt Cloud to push code to the repo
* I defined a schema for the staging area in the dwh that will house staging views:

    ```yml
    version: 2

    sources:
        - name: staging
          database: data-eng-zoomcamp-339102
          schema: trips_data_all

          tables:
            - name: green_tripdata_partitoned
            - name: yellow_tripdata_partitoned
    ```

   and generated an intial query using jinja templating.

   ```python
   {{ config(materialized='view') }}

    select * from {{ source('staging', 'green_tripdata_partitoned') }}

    limit 100
    ```


    
* Running the model gave me an error because dbt tried creating a new schema on my bq called `dbt_mharty` in multi-region US, which is different from the trips_data_all_schema:
`404 Not found: Dataset data-eng-zoomcamp-339102:trips_data_all was not found in location US`
This was fixed by deleting the schema in the google cloud console and then re-creating it with the same name and defining the location to be the same as the location of my `trips_data_all` schema. (`us-central1`). dbt created the schema using default location and it was wrong. 
* After fixing this error. It works and creates a view.
* The next step is to replace the * in the query to select the columns. Here we are using `cast()` statements to set the proper datatypes, and renaming some columns to match between datasets. For example renaming `lpep_dropoff_time` to `dropoff_time.

#### Macros

* Macros are like functions in other programming languages. They take inputs and return compiled code. They are used to abstract reusable sql code into reusable chunks
* Below is an example declaration of a macro called `get_payment_type_description`. It takes the input of `payment_type` and generates a case when statement in SQL.

  ```python
  {#
      This is a comment block
      This macro returns the description of the payment_type 
  #}

  {% macro get_payment_type_description(payment_type) -%}

      case {{ payment_type }}
          when 1 then 'Credit card'
          when 2 then 'Cash'
          when 3 then 'No charge'
          when 4 then 'Dispute'
          when 5 then 'Unknown'
          when 6 then 'Voided trip'
      end

  {%- endmacro %}
  ```

* Macros are stored in the macros directory of the dbt project

#### Packages

* Similar to python packages, there are dbt packages that contain useful macros. You can find packages on the dbt package hub or on github
* To use packages, we need to define a packages.yml file in the root of the project:

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

* Then run the command `dbt_deps` which will scan the packages.yml file and install all required dependencies
* We use the `dbt_utils.surrogate_key` macro to create a unique identifier for each trip

#### Variables

* Variables can be defined in the project.yml file or as command line arguments (using the --var flag) when running dbt models.
* We will use a variable in our case to distinguish between a testing run or a run on the full dataset by placing an if statement in a code chunk with the limit statement.
Now if we use the dbt run command with the flag `--var 'is_test_run: false'`, it will not insert the limit statement 


```yml
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

#### Yellow trip data
* Copy and paste the model from the course github repo for the yellow taxi data
* I commented out the ehail column, because I am removing that data

#### Seeds
* Seeds are CSV files in your dbt project (typically in your seeds directory), that dbt can load into your data warehouse using the dbt seed command.
* Seeds can be referenced in downstream models the same way as referencing models — by using the ref function.
* Because these CSV files are located in your dbt repository, they are version controlled and code reviewable. Seeds are best suited to static data which changes infrequently.
* Good use-cases for seeds:
  * A list of mappings of country codes to country names
  * A list of test emails to exclude from analysis
  * A list of employee account IDs

* Poor use-cases of dbt seeds:
  * Loading raw data that has been exported to CSVs
  * Any kind of production data containing sensitive information. For example personal identifiable information (PII) and passwords.

* We will load the mappings from taxi data zone id to taxi zone name as a seed
* In the cloud, there is no convenient way to upload csv files, so create an empty file, and copy paste the data in there.
* The file is [here](mharty3/data_engineering_zoomcamp_2022/week01/docker-sql/taxi+_zone_lookup.csv)
* Run the command `dbt seed`. It will create the table in the data warehouse determining data types by default
* If we need to, we can define the datatypes in the `dbt_project.yml` file
* if you change the seed file and need to do a full refresh of the table, run `dbt seed --full-refresh`
* Now we can create a model based on the seed. Create the model `dim_zones.sql` in the `core` directory


#### 
