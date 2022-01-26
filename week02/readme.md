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