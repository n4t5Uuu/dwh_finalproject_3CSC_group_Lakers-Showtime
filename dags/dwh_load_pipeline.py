from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


sys.path.append("/sql")

# -------------------------------
# IMPORT LOADER SCRIPTS
# -------------------------------
from loader.load_dim_date import main as load_dim_date
from loader.load_dim_user import load_dim_user
from loader.load_dim_merchant import load_dim_merchant
from loader.load_dim_staff import load_dim_staff
from loader.load_dim_campaign import load_dim_campaign

from loader.load_fact_orders import load_fact_orders
from loader.load_fact_line_item import load_fact_line_item
from loader.load_fact_campaign_availed import load_fact_campaign_availed


# =====================================================
#            DWH LOAD DAG (LOADING ONLY)
# =====================================================
with DAG(
    dag_id="dwh_load_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "loading"],
) as dag:


    # ----------------------------------------------------
    # DIMENSION LOAD TASKS
    # ----------------------------------------------------
    task_load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date,
    )

    task_load_dim_user = PythonOperator(
        task_id="load_dim_user",
        python_callable=load_dim_user,
    )

    task_load_dim_merchant = PythonOperator(
        task_id="load_dim_merchant",
        python_callable=load_dim_merchant,
    )

    task_load_dim_staff = PythonOperator(
        task_id="load_dim_staff",
        python_callable=load_dim_staff,
    )

    task_load_dim_campaign = PythonOperator(
        task_id="load_dim_campaign",
        python_callable=load_dim_campaign,
    )


    # ----------------------------------------------------
    # FACT LOAD TASKS
    # ----------------------------------------------------
    task_load_fact_orders = PythonOperator(
        task_id="load_fact_orders",
        python_callable=load_fact_orders,
    )

    task_load_fact_line_item = PythonOperator(
        task_id="load_fact_line_item",
        python_callable=load_fact_line_item,
    )

    task_load_fact_campaign_availed = PythonOperator(
        task_id="load_fact_campaign_availed",
        python_callable=load_fact_campaign_availed,
    )


    # =====================================================
    #               PIPELINE DEPENDENCY CHAIN
    # =====================================================

    # Load dimensions in correct order
    task_load_dim_date >> task_load_dim_user
    task_load_dim_user >> [task_load_dim_merchant, task_load_dim_staff]
    [task_load_dim_merchant, task_load_dim_staff] >> task_load_dim_campaign

    # After all dims → load factOrders + factLineItem
    task_load_dim_campaign >> [
        task_load_fact_orders,
        task_load_fact_line_item,
    ]

    # factCampaignAvailed depends on factOrders
    task_load_fact_orders >> task_load_fact_campaign_availed
