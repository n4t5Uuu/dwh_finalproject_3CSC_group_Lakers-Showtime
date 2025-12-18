from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="dag_build_all_dim",
    start_date=datetime(2025, 1, 1),
    template_searchpath=["/sql"],
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimensions", "kimball"],
) as dag:
    
    create_schema_shopzada = PostgresOperator(
        task_id="create_schema_shopzada",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;
        """
    )

    create_dim_indexes = PostgresOperator(
        task_id="create_dim_indexes",
        postgres_conn_id="postgres_default",
        sql="""
        -- USER SCD2
        CREATE INDEX IF NOT EXISTS idx_dim_user_scd
            ON shopzada.dim_user (user_id, effective_from, effective_to);

        -- STAFF SCD2
        CREATE INDEX IF NOT EXISTS idx_dim_staff_scd
            ON shopzada.dim_staff (staff_id, effective_from, effective_to);

        -- MERCHANT SCD2
        CREATE INDEX IF NOT EXISTS idx_dim_merchant_scd
            ON shopzada.dim_merchant (merchant_id, effective_from, effective_to);
        """
    )


    build_dim_campaign = PostgresOperator(
        task_id="build_dim_campaign",
        postgres_conn_id="postgres_default",
        sql="/build_dim_campaign.sql",
    )

    build_dim_date = PostgresOperator(
        task_id="build_dim_date",
        postgres_conn_id="postgres_default",
        sql="/build_dim_date.sql",
    )

    build_dim_merchant = PostgresOperator(
        task_id="build_dim_merchant",
        postgres_conn_id="postgres_default",
        sql="/build_dim_merchant.sql",
    )

    build_dim_user = PostgresOperator(
        task_id="build_dim_user",
        postgres_conn_id="postgres_default",
        sql="/build_dim_user.sql",
    )

    build_dim_product = PostgresOperator(
        task_id="build_dim_product",
        postgres_conn_id="postgres_default",
        sql="/build_dim_product.sql",
    )

    build_dim_staff = PostgresOperator(
        task_id="build_dim_staff",
        postgres_conn_id="postgres_default",
        sql="/build_dim_staff.sql",
    )

    create_schema_shopzada >> [
        build_dim_campaign, build_dim_date, build_dim_merchant, 
        build_dim_user, build_dim_product, build_dim_staff
        ] >> create_dim_indexes