from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

SCRIPTS_PATH = Path("/scripts/marketing_scripts")  # path inside container
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

from marketing_clean import main as marketing_ingest_main

with DAG(
    dag_id="dag_marketing_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["ingest", "marketing"],
) as dag:
    cleaning_task = PythonOperator(
        task_id="run_marketing_cleaning",
        python_callable=marketing_ingest_main,
    )