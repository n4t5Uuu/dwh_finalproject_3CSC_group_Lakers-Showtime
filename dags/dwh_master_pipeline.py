from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dwh_master_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "master"],
) as dag:

    # 1 — DIM DATE FIRST
    dim_date = TriggerDagRunOperator(
        task_id="run_dim_date_pipeline",
        trigger_dag_id="dwh_dim_date_pipeline",
        wait_for_completion=True
    )

    # 2 — DIMENSION TABLES (can run in parallel)
    dim_user = TriggerDagRunOperator(
        task_id="run_user_pipeline",
        trigger_dag_id="dwh_user_pipeline",
        wait_for_completion=True
    )

    dim_product = TriggerDagRunOperator(
        task_id="run_product_pipeline",
        trigger_dag_id="dwh_product_pipeline",
        wait_for_completion=True
    )

    dim_merchant = TriggerDagRunOperator(
        task_id="run_merchant_pipeline",
        trigger_dag_id="dwh_merchant_pipeline",
        wait_for_completion=True
    )

    dim_staff = TriggerDagRunOperator(
        task_id="run_staff_pipeline",
        trigger_dag_id="dwh_staff_pipeline",
        wait_for_completion=True
    )

    dim_campaign = TriggerDagRunOperator(
        task_id="run_campaign_pipeline",
        trigger_dag_id="dwh_campaign_pipeline",
        wait_for_completion=True
    )

    # 3 — FACT TABLES (run after all dims)
    fact_orders = TriggerDagRunOperator(
        task_id="run_fact_orders_pipeline",
        trigger_dag_id="dwh_fact_orders_pipeline",
        wait_for_completion=True
    )

    fact_lineitem = TriggerDagRunOperator(
        task_id="run_fact_lineitem_pipeline",
        trigger_dag_id="dwh_fact_lineitem_pipeline",
        wait_for_completion=True
    )

    fact_campaign = TriggerDagRunOperator(
        task_id="run_fact_campaign_pipeline",
        trigger_dag_id="dwh_fact_campaign_pipeline",
        wait_for_completion=True
    )

    # ───────────────────────────────
    # DEPENDENCIES
    # ───────────────────────────────

    dim_date >> [dim_user, dim_product, dim_merchant, dim_staff, dim_campaign]

    [dim_user, dim_product, dim_merchant, dim_staff, dim_campaign] >> fact_orders
    fact_orders >> [fact_lineitem, fact_campaign]
