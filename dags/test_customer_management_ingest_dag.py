from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

SCRIPTS_PATH = Path("/scripts/customer_management_scripts")  # path inside container
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.append(str(SCRIPTS_PATH))

# Import scripts
from user_credit_card_clean import main as credit_card_clean_main
from user_job_clean import main as job_clean_main
from step_customer_management_ingest import main as customer_management_ingest_main

with DAG(
    dag_id="step_customer_management_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["ingest", "customermanagement"],
) as dag:

    # Task 1: Clean credit card data
    credit_card_clean_task = PythonOperator(
        task_id="clean_user_credit_cards",
        python_callable=credit_card_clean_main,
    )

    # Task 2: Clean job data
    job_clean_task = PythonOperator(
        task_id="clean_user_job_data",
        python_callable=job_clean_main,
    )

    # Task 3: Main ingest (depends on the cleaned files)
    ingest_main_task = PythonOperator(
        task_id="run_customer_management_cleaning",
        python_callable=customer_management_ingest_main,
    )

    # Dependencies: Task 1 & 2 → Task 3
    [credit_card_clean_task, job_clean_task] >> ingest_main_task