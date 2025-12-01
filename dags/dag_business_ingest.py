from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Ensure /scripts is on sys.path so we can import business_ingest
SCRIPTS_PATH = Path("/scripts/business_scripts")
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

from business_clean import main as business_ingest_main  # noqa: E402

with DAG(
    dag_id="dag_business_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # only run manually
    catchup=False,
    tags=["ingest", "business"],
) as dag:

    run_business_ingest = PythonOperator(
        task_id="business_ingest",
        python_callable=business_ingest_main,
    )
