from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_dim_build_product",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimension", "product"],
) as dag:

    # -------------------------------------------------
    # 1. CREATE DIM_PRODUCT TABLE
    # -------------------------------------------------
    create_dim_product = PostgresOperator(
        task_id="create_dim_product",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.dim_product (
            product_key SERIAL PRIMARY KEY,
            product_id VARCHAR(30) NOT NULL,
            product_name VARCHAR(255),
            product_type VARCHAR(100),
            price NUMERIC(10,2),
            UNIQUE (product_id, product_name)
        );
        """
    )

    # -------------------------------------------------
    # 2. LOAD DIM_PRODUCT (TYPE 1)
    # -------------------------------------------------
    load_dim_product = PostgresOperator(
        task_id="load_dim_product",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.dim_product;

        INSERT INTO shopzada.dim_product (
            product_id,
            product_name,
            product_type,
            price
        )
        SELECT
            p.product_id,
            p.product_name,
            p.product_type,
            p.price
        FROM staging.product_list p;
        """
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    create_dim_product >> load_dim_product
