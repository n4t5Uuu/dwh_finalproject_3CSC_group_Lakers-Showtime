from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
import sys

# =====================================================
# IMPORT CLEANING SCRIPT
# =====================================================
sys.path.append("/scripts")
from marketing_scripts.campaign_clean import main as clean_campaign_data


# =====================================================
# PYTHON: LOAD CLEAN DATA → STAGING
# =====================================================

def load_staging_campaign():
    df = pd.read_csv("/clean_data/marketing/campaign_data.csv")

    # 🔒 Explicit column lock (order + count)
    insert_cols = [
        "campaign_id",
        "campaign_name",
        "campaign_description",
        "discount_pct",
    ]

    df = df.loc[:, insert_cols]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.campaign_data;")

    execute_values(
        cur,
        f"""
        INSERT INTO staging.campaign_data (
            {", ".join(insert_cols)}
        ) VALUES %s
        """,
        df.itertuples(index=False, name=None)
    )

    conn.commit()
    cur.close()
    conn.close()


# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_stage_campaign",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "campaign"],
) as dag:

    # -------------------------------------------------
    # 1. RUN CLEANING SCRIPT
    # -------------------------------------------------
    run_cleaning = PythonOperator(
        task_id="clean_campaign_data",
        python_callable=clean_campaign_data,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging_table = PostgresOperator(
        task_id="create_staging_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.campaign_data (
            campaign_id VARCHAR(30),
            campaign_name VARCHAR(255),
            campaign_description TEXT,
            discount_pct INTEGER
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD CLEAN DATA → STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_campaign",
        python_callable=load_staging_campaign,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    run_cleaning >> create_staging_table >> load_staging
