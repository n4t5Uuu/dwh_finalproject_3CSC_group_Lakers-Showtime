from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

SCRIPTS_PATH = Path("/scripts/customer_management_scripts")  # path inside container
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

from step_customer_management_ingest import main as customer_management_ingest_main

with DAG(
    dag_id="step_customer_management_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["ingest", "customermanagement"],
) as dag:
    cleaning_task = PythonOperator(
        task_id="run_customer_management_cleaning",
        python_callable=customer_management_ingest_main,
    )
