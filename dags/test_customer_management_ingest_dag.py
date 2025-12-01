from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

# Path inside the container where your scripts live
SCRIPTS_PATH = Path("/scripts/customer_management_scripts")
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

# Import the combined cleaning / ingest script
from customer_management_clean import main as customer_management_clean_main

with DAG(
    dag_id="step_customer_management_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["ingest", "customermanagement"],
) as dag:

    run_customer_management_clean = PythonOperator(
        task_id="run_customer_management_clean",
        python_callable=customer_management_clean_main,
    )
