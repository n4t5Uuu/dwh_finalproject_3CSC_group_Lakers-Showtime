from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dag_master_shopzada_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["master", "shopzada", "dwh"],
) as dag:

    # =========================================================
    # CLEANING LAYER
    # =========================================================

    clean_all_sources = TriggerDagRunOperator(
        task_id="clean_all_sources",
        trigger_dag_id="dag_clean_all_sources",
        wait_for_completion=True,
    )

    # =========================================================
    # STAGING LAYER
    # =========================================================

    stage_all_sources = TriggerDagRunOperator(
        task_id="stage_all_sources",
        trigger_dag_id="dag_stage_all_sources",
        wait_for_completion=True,
    )

    # =========================================================
    # DIMENSIONS
    # =========================================================

    build_all_dim = TriggerDagRunOperator(
        task_id="build_all_dim",
        trigger_dag_id="dag_build_all_dim",
        wait_for_completion=True,
    )

    # =========================================================
    # FACT TABLES
    # =========================================================

    fact_line_item = TriggerDagRunOperator(
        task_id="build_fact_line_item",
        trigger_dag_id="dag_build_fact_line_item",
        wait_for_completion=True,
    )

    fact_orders = TriggerDagRunOperator(
        task_id="build_fact_orders",
        trigger_dag_id="dag_build_fact_orders",
        wait_for_completion=True,
    )

    fact_campaign = TriggerDagRunOperator(
        task_id="build_fact_campaign_availed",
        trigger_dag_id="dag_build_fact_campaign_availed",
        wait_for_completion=True,
    )


    # Cleaning -> Staging -> Dimensions -> Facts
    clean_all_sources >> stage_all_sources >> build_all_dim >> [ fact_line_item, fact_orders, fact_campaign]

