from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values

# =====================================================
# PYTHON: LOAD CLEAN CSV -> STAGING
# =====================================================

def load_staging_order_with_merchant_clean():
    df = pd.read_csv("/clean_data/enterprise/order_with_merchant_clean.csv")

    df = df[[
        "order_id",
        "merchant_id",
        "staff_id"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.order_with_merchant_clean;")

    execute_values(
        cur,
        """
        INSERT INTO staging.order_with_merchant_clean (
            order_id,
            merchant_id,
            staff_id
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()

# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id="dag_stage_enterprise_order_with_merchant",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "enterprise"],
) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_order_with_merchant_clean",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.order_with_merchant_clean (
            order_id VARCHAR(100),
            merchant_id VARCHAR(40),
            staff_id VARCHAR(40)
        );
        """
    )

    load_staging = PythonOperator(
        task_id="load_staging_order_with_merchant_clean",
        python_callable=load_staging_order_with_merchant_clean,
    )

    create_staging_table >> load_staging
