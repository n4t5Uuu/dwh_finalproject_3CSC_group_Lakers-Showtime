from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

SCRIPTS_PATH = Path("/scripts/operation_scripts")
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

from operation_ingest import main as operation_ingest_main  # noqa: E402

with DAG(
    dag_id="step_operation_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ingest", "operations"],
) as dag:

    run_operation_ingest = PythonOperator(
        task_id="run_operation_ingest",
        python_callable=operation_ingest_main,
    )
