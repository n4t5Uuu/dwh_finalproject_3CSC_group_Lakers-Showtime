from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

# Allow Airflow to import scripts
SCRIPT_PATH = Path("/opt/airflow/scripts/operation_scripts")
sys.path.append(str(SCRIPT_PATH))

from operation_ingest import main as ingest_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="operations_line_item_ingest",
    description="Cleans & joins Operations Department line item data",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_line_items_task = PythonOperator(
        task_id="run_operation_ingest",
        python_callable=ingest_main,
    )

    ingest_line_items_task
