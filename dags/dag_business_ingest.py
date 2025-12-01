from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Ensure /scripts is on sys.path so we can import business_ingest
SCRIPTS_PATH = Path("/scripts/business_scripts")
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

from business_clean import main as customer_management_ingest_main  # noqa: E402

with DAG(
    dag_id="dag_customer_management_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # only run manually
    catchup=False,
    tags=["ingest", "customer_management"],
) as dag:

    run_business_ingest = PythonOperator(
        task_id="customer_management_ingest",
        python_callable=customer_management_ingest_main,
    )
