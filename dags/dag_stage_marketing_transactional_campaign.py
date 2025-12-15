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

def load_staging_transactional_campaign_clean():
    df = pd.read_csv("/clean_data/marketing/transactional_campaign_clean.csv")

    # Normalize naming to match fact build contract
    # (keep your cleaning output intact, standardize here)
    if "transaction_date_key" in df.columns and "date_key" not in df.columns:
        df = df.rename(columns={"transaction_date_key": "date_key"})

    # Ensure types
    df["availed"] = pd.to_numeric(df["availed"], errors="coerce").fillna(0).astype(int)

    df = df[[
        "order_id",
        "campaign_id",
        "transaction_date",
        "date_key",
        "estimated_arrival_days",
        "availed"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.transactional_campaign_clean;")

    execute_values(
        cur,
        """
        INSERT INTO staging.transactional_campaign_clean (
            order_id,
            campaign_id,
            transaction_date,
            date_key,
            estimated_arrival_days,
            availed
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
    dag_id="dag_stage_marketing_transactional_campaign",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "marketing"],
) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_transactional_campaign_clean",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.transactional_campaign_clean (
            order_id VARCHAR(100),
            campaign_id VARCHAR(40),
            transaction_date DATE,
            date_key INT,
            estimated_arrival_days INT,
            availed INT
        );
        """
    )

    load_staging = PythonOperator(
        task_id="load_staging_transactional_campaign_clean",
        python_callable=load_staging_transactional_campaign_clean,
    )

    create_staging_table >> load_staging
