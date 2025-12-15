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

    clean_operations = TriggerDagRunOperator(
        task_id="clean_operations",
        trigger_dag_id="dag_clean_operations",
        wait_for_completion=True,
    )

    clean_marketing = TriggerDagRunOperator(
        task_id="clean_marketing_transactional",
        trigger_dag_id="dag_clean_marketing_transactional",
        wait_for_completion=True,
    )

    clean_enterprise = TriggerDagRunOperator(
        task_id="clean_enterprise_order_with_merchant",
        trigger_dag_id="dag_clean_enterprise_order_with_merchant",
        wait_for_completion=True,
    )

    # =========================================================
    # STAGING LAYER
    # =========================================================

    stage_customer = TriggerDagRunOperator(
        task_id="stage_customer_management",
        trigger_dag_id="dag_stage_customer_management",
        wait_for_completion=True,
    )

    stage_business = TriggerDagRunOperator(
        task_id="stage_business_product",
        trigger_dag_id="dag_stage_business_product",
        wait_for_completion=True,
    )

    stage_operations = TriggerDagRunOperator(
        task_id="stage_operations_orders",
        trigger_dag_id="dag_stage_operations_orders",
        wait_for_completion=True,
    )

    stage_enterprise = TriggerDagRunOperator(
        task_id="stage_enterprise_order_with_merchant",
        trigger_dag_id="dag_stage_enterprise_order_with_merchant",
        wait_for_completion=True,
    )

    stage_campaign_dim = TriggerDagRunOperator(
        task_id="stage_campaign_dimension",
        trigger_dag_id="dag_stage_campaign",  
        wait_for_completion=True,
    )

    stage_marketing = TriggerDagRunOperator(
        task_id="stage_marketing_transactional_campaign",
        trigger_dag_id="dag_stage_marketing_transactional_campaign",
        wait_for_completion=True,
    )

    stage_fact_src = TriggerDagRunOperator(
        task_id="stage_fact_line_item_src",
        trigger_dag_id="dag_stage_fact_line_item_src",
        wait_for_completion=True,
    )

    stage_merchant = TriggerDagRunOperator(
        task_id="stage_merchant",
        trigger_dag_id="dag_stage_merchant",
        wait_for_completion=True,
    )

    stage_staff = TriggerDagRunOperator(
        task_id="stage_staff",
        trigger_dag_id="dag_stage_staff",
        wait_for_completion=True,
    )



    # =========================================================
    # DIMENSIONS
    # =========================================================

    dim_date = TriggerDagRunOperator(
        task_id="build_dim_date",
        trigger_dag_id="dag_build_dim_date",
        wait_for_completion=True,
    )

    dim_user = TriggerDagRunOperator(
        task_id="build_dim_user",
        trigger_dag_id="dag_build_dim_user",
        wait_for_completion=True,
    )

    dim_product = TriggerDagRunOperator(
        task_id="build_dim_product",
        trigger_dag_id="dag_build_dim_product",
        wait_for_completion=True,
    )

    dim_merchant = TriggerDagRunOperator(
        task_id="build_dim_merchant",
        trigger_dag_id="dag_build_dim_merchant",
        wait_for_completion=True,
    )

    dim_staff = TriggerDagRunOperator(
        task_id="build_dim_staff",
        trigger_dag_id="dag_build_dim_staff",
        wait_for_completion=True,
    )

    dim_campaign = TriggerDagRunOperator(
        task_id="build_dim_campaign",
        trigger_dag_id="dag_build_dim_campaign",
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

    # =========================================================
    # DEPENDENCIES
    # =========================================================

    # Cleaning
    [clean_operations, clean_marketing, clean_enterprise]

    # Cleaning → Staging
    clean_operations >> stage_operations
    clean_marketing >> stage_marketing
    clean_enterprise >> stage_enterprise

    # Staging
    stage_operations >> stage_fact_src
    stage_customer >> dim_user
    stage_business >> dim_product
    stage_enterprise >> stage_merchant >> dim_merchant 
    stage_staff >> dim_staff

    # Dimensions
    stage_fact_src >> dim_date
    [dim_user, dim_product, dim_merchant, dim_staff, dim_campaign, dim_date]

    # Dimensions → Facts
    [
        dim_user,
        dim_product,
        dim_merchant,
        dim_staff,
        stage_campaign_dim >> dim_campaign,
        dim_date,
        stage_fact_src,
        stage_operations,
    ] >> fact_line_item >> fact_orders >> fact_campaign
