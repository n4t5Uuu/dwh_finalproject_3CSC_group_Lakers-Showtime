from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append("/scripts")

from business_scripts.business_clean import main as business_main
from customer_management_scripts.customer_management_clean import main as cm_main
from enterprise_scripts.enterprise_clean import main as enterprise_main
from marketing_scripts.marketing_clean import main as marketing_main
from operation_scripts.operation_clean import main as operation_main
from build_dim_date import main as dim_date_main
from build_fact_orders import main as fact_main
from setup_directories import main as setup_dirs_main

# -----------------------------------------
# DAG DEFINITION
# -----------------------------------------
with DAG(
    dag_id="dwh_master_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh"],
) as dag:

    task_business = PythonOperator(
        task_id="ingest_business",
        python_callable=business_main,
    )

    task_customer = PythonOperator(
        task_id="ingest_customer_management",
        python_callable=cm_main,
    )

    task_enterprise = PythonOperator(
        task_id="ingest_enterprise",
        python_callable=enterprise_main,
    )

    task_marketing = PythonOperator(
        task_id="ingest_marketing",
        python_callable=marketing_main,
    )

    task_operations = PythonOperator(
        task_id="ingest_operations",
        python_callable=operation_main,
    )

    task_fact_orders = PythonOperator(
        task_id="build_fact_orders",
        python_callable=fact_main,
    )

    setup_dirs = PythonOperator(
        task_id="setup_directories",
        python_callable=setup_dirs_main
    )

    task_build_dim_date = PythonOperator(
        task_id="build_dim_date",
        python_callable=dim_date_main,
    )

    # ----- DEFINE PIPELINE ORDER -----
    setup_dirs >> task_build_dim_date >> task_business >> task_customer >> task_enterprise >> task_marketing >> task_operations >> task_fact_orders
