import glob
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

INCOMING_DIR = "/opt/airflow/data/incoming"
FILE_MASK = f"{INCOMING_DIR}/*.csv"


def find_file():
    files = sorted(glob.glob(FILE_MASK))
    return files[0] if files else None


def is_csv_empty(path):
    try:
        return pd.read_csv(path).empty
    except pd.errors.EmptyDataError:
        return True


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
        return "process_data"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "File is empty, nothing to process"',
    )

    process_data = EmptyOperator(task_id="process_data")

    wait_for_file >> check_file() >> [log_empty_file, process_data]