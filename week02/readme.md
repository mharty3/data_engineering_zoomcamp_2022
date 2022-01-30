# Week 02: Data Ingestion

## Data Lakes (Google Cloud Storage)

- [Video](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14) | [Slides](https://docs.google.com/presentation/d/1RkH-YhBz2apIjYZAxUz2Uks4Pt51-fVWVN9CcH9ckyY/edit?usp=sharing)
- Data Lakes are meant to store both structured and unstructured data at unlimited scale
- It is often used to store raw or minimally processed data such as log data or sensor data
- Started as a solution for companies to store large amounts of data quickly
- ETL (extract transform load) vs. ELT (extract load transform)
  - ETL is better for smaller amounts of data, ELT is common in data lakes (schema on read)
- Contrast to a data warehouse that contains highly structured, relational data

## Workflow Orchestration

- [Video](https://www.youtube.com/watch?v=0yK7LXwYeD0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=16)
- Our pipeline last week in the python script used wget to download the data as a csv, and then write the data to the database. Turns out this is a bad strategy
  - It's better to have each step separated in case one step fails, the entire pipeline doesn't fail
- The steps in a data pipeline can be defined as a directed acyclic graph or **DAG**
  - Directed: data flows from one step to the next
  - Acyclic: there are no cycles, data flows in one direction only
  - Graph: processing steps are the nodes, and data products are the edges
- Workflow orchestration tools such as Airflow can be used to define these **DAGs** and conduct the workflow

![dag example](img/DAG.png)

## Set up Airflow environment using Docker
* [Video](https://www.youtube.com/watch?v=lqDMzReAtrw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=18)
* Airflow is a tool used to schedule and orchestrate workflows and data pipelines [Here is another good overview video](https://www.youtube.com/watch?v=QgzkB1hcq5s)
* This step is setting up the airflow environment to run on google cloud. We will use the default docker-compose.yaml from airflow with a custom dockerfile to install some dependecies including gcloud. By the end of this part of the workshop, we will be able to connect to the airflow webserver.

* To be consistent with the workshop I moved the google credentials file to this location:

    ```bash
    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
    ```

* Create an airflow sub-directory and download the airflow image
  `wget https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml -O airflow/docker-compose.yml`

* Set the Airflow user:

  On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:

  ```
  mkdir -p ./dags ./logs ./plugins

  set AIRFLOW_UID $(id -u)
  ```

  I couldn't get this to work for me for some reason, so I hard coded my userid (1002) into the Dockerfile

* Prepare for Docker Build:
  * The default configuration of the docker-compose will not work for us because it is not configured to work with google cloud and other dependencies. We need to create a custom docker file specifying these. The docker compose file explains this:

  ```yaml
    # In order to add custom dependencies or upgrade provider packages you can use your extended image.
    # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
    # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
    image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.2.3}
    # build: .
  ```
  We also need a `requirements.txt` file that specifies the dependencies of our DAGs. Add that file with these dependencies: 
  
  ```yaml
  apache-airflow-providers-google
  pyarrow
  ```

  These will be installed in the dockerfile. Instructions to install and set up gcloud dependencies are [here](https://airflow.apache.org/docs/docker-stack/recipes.html)

  In the docker-compose file, set `AIRFLOW__CORE__LOAD_EXAMPLES` to `false`. That way airflow does not install any example dags into our directory.

  Add the following environment variables to the environment in `$airflow-common-env`. The project ID and GCS bucket were created last week.

  ```yaml
    GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'
    GCP_PROJECT_ID: 'data-eng-zoomcamp-339102'
    GCP_GCS_BUCKET: "dtc_data_lake_data-eng-zoomcamp-339102"
  ```

  In volumes, mount the gcp credentials directory as read only:

  `- ~/.google/credentials/:/.google/credentials:ro`

  * Run `docker-compose build`
  * Run `docker-compose up airflow-init` to run the initialization step. This will run database migrations and create the first user account with the username `airflow` and the password `airflow`
  * Run `docker-compose up` to start up airflow
  * Forward the port using VSCode and connect to localhost:8080 to see the airflow webserver

# Ingesting Data to GCP with Airflow
* [Video](https://www.youtube.com/watch?v=9ksX9REfL8w&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19)



