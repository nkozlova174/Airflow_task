import glob

import pandas as pd

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from transformers import DataTransformer

INCOMING_DIR = "/opt/airflow/data/incoming"
FILE_MASK = f"{INCOMING_DIR}/*.csv"

data_transformer = DataTransformer()


def find_file():
    files = sorted(glob.glob(FILE_MASK))
    return files[0] if files else None


def is_csv_empty(path):
    try:
        return pd.read_csv(path).empty
    except pd.errors.EmptyDataError:
        return True


def null_task():
    path = find_file()
    df = pd.read_csv(path)
    data_transformer.replace_nulls(df).to_csv(path, index=False)

def sort_task():
    path = find_file()
    df = pd.read_csv(path)
    data_transformer.sort_by_create_date(df).to_csv(path, index=False)

def clean_task():
    path = find_file()
    df = pd.read_csv(path)
    data_transformer.clean_content(df).to_csv(path, index=False)


with DAG(
    dag_id="transform_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_MASK,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )

    @task.branch
    def check_file():
        path = find_file()
        if path is None or is_csv_empty(path):
            return "log_empty_file"
        return "data_transformation.sort_by_created_date"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "File is empty, nothing to process"',
    )

    with TaskGroup(group_id = "data_transformation") as data_transformation:

        sort = PythonOperator(
            task_id="sort_by_created_date",
            python_callable=sort_task,
        )

        nulls = PythonOperator(
            task_id="replace_nulls",
            python_callable=null_task,
        )

        clean = PythonOperator(
            task_id="clean_content",
            python_callable=clean_task,
        )
        sort >> nulls >>clean


    wait_for_file >> check_file() >> [log_empty_file, data_transformation]