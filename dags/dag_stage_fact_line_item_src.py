from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import sys
from psycopg2.extras import execute_values

# =====================================================
# IMPORT FACT SOURCE BUILDER
# =====================================================
sys.path.append("/scripts")
from build_fact_line_item_src import main as build_fact_line_item_src


# =====================================================
# LOAD STAGING TABLE
# =====================================================
def load_staging_fact_line_item_src():
    df = pd.read_csv("/clean_data/facts/fact_line_item_src.csv")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.fact_line_item_src;")

    execute_values(
        cur,
        """
        INSERT INTO staging.fact_line_item_src (
            order_id,
            product_id,
            product_name,
            user_id,
            merchant_id,
            staff_id,
            unit_price,
            quantity,
            line_amount,
            date_key,
            campaign_id
        ) VALUES %s
        """,
        df[[
            "order_id",
            "product_id",
            "product_name",
            "user_id",
            "merchant_id",
            "staff_id",
            "unit_price",
            "quantity",
            "line_amount",
            "date_key",
            "campaign_id",
        ]].values.tolist()
    )

    conn.commit()
    cur.close()


# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id="dag_stage_fact_line_item_src",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "fact", "staging"],
) as dag:

    # -------------------------------------------------
    # 1. BUILD FACT SOURCE (PYTHON)
    # -------------------------------------------------
    build_src = PythonOperator(
        task_id="build_fact_line_item_src",
        python_callable=build_fact_line_item_src,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging = PostgresOperator(
        task_id="create_staging_fact_line_item_src",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.fact_line_item_src (
            order_id     VARCHAR(100),
            product_id   VARCHAR(50),
            product_name VARCHAR(255),

            user_id      VARCHAR(30),
            merchant_id  VARCHAR(30),
            staff_id     VARCHAR(30),

            unit_price   NUMERIC(12,2),
            quantity     INT,
            line_amount  NUMERIC(14,2),

            date_key     INT,
            campaign_id  VARCHAR(40)
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_fact_line_item_src",
        python_callable=load_staging_fact_line_item_src,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    build_src >> create_staging >> load_staging
