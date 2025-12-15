from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# =====================================================
# IMPORT CLEANING SCRIPT
# =====================================================

sys.path.append("/scripts")

from enterprise_scripts.order_with_merchant_clean import main as clean_order_with_merchant

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_clean_enterprise_order_with_merchant",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "cleaning", "enterprise"],
) as dag:

    clean_order_with_merchant_task = PythonOperator(
        task_id="clean_order_with_merchant",
        python_callable=clean_order_with_merchant,
    )
