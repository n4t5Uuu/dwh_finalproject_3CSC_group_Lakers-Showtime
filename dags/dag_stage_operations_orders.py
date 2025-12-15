from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values

# =====================================================
# LOADERS: CLEAN CSV -> STAGING
# =====================================================

def load_staging_orders_clean():
    df = pd.read_csv("/clean_data/operations/orders_clean.csv")

    # ✅ Normalize date key naming at STAGING level
    df = df.rename(columns={
        "transaction_date_key": "date_key"
    })

    df = df[[
        "order_id",
        "user_id",
        "transaction_date",
        "date_key",
        "estimated_arrival_days"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.orders_clean;")

    execute_values(
        cur,
        """
        INSERT INTO staging.orders_clean (
            order_id,
            user_id,
            transaction_date,
            date_key,
            estimated_arrival_days
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()


def load_staging_order_delays_clean():
    df = pd.read_csv("/clean_data/operations/order_delays_clean.csv")

    df = df[[
        "order_id",
        "delay_in_days"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.order_delays_clean;")

    execute_values(
        cur,
        """
        INSERT INTO staging.order_delays_clean (
            order_id,
            delay_in_days
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()


# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_stage_operations_orders",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "operations"],
) as dag:

    create_staging_tables = PostgresOperator(
        task_id="create_staging_orders_tables",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.orders_clean (
            order_id VARCHAR(100),
            user_id VARCHAR(30),
            transaction_date DATE,
            date_key INT,
            estimated_arrival_days INT
        );

        CREATE TABLE IF NOT EXISTS staging.order_delays_clean (
            order_id VARCHAR(100),
            delay_in_days INT
        );
        """
    )

    load_orders = PythonOperator(
        task_id="load_staging_orders_clean",
        python_callable=load_staging_orders_clean
    )

    load_delays = PythonOperator(
        task_id="load_staging_order_delays_clean",
        python_callable=load_staging_order_delays_clean
    )

    create_staging_tables >> [load_orders, load_delays]
