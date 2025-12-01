from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

# Add operation_scripts to sys.path for import
SCRIPTS_PATH = Path("/scripts/operation_scripts")
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

# Import the main function from your cleaning script
from operation_clean import main as operation_ingest_main  # noqa: E402

with DAG(
    dag_id="operation_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual run
    catchup=False,
    tags=["ingest", "operations"],
) as dag:

    run_operation_ingest = PythonOperator(
        task_id="run_operation_ingest",
        python_callable=operation_ingest_main,
    )
    
