from datetime import datetime
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")



local_workflow = DAG(
    "Local_Ingestion_Dag",
    schedule_interval="0 6 2 * *", # https://crontab.guru/#0_6_2_*_*
    start_date=datetime(2021, 1, 1)
)

url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {url} > {AIRFLOW_HOME}/output.csv'
    )

    ingest_task = BashOperator(
        task_id='ingest',
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task