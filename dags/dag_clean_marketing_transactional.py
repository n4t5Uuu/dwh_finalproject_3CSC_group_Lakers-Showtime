from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# =====================================================
# IMPORT CLEANING SCRIPTS
# =====================================================

sys.path.append("/scripts")

from marketing_scripts.transactional_campaign_clean import main as clean_transactional_campaign

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_clean_marketing_transactional",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "cleaning", "marketing"],
) as dag:

    # -------------------------------------------------
    # CLEAN TRANSACTIONAL CAMPAIGN DATA (FACT SOURCE)
    # -------------------------------------------------
    clean_transactional_campaign_task = PythonOperator(
        task_id="clean_transactional_campaign",
        python_callable=clean_transactional_campaign,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    clean_transactional_campaign_task
