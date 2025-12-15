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
from enterprise_scripts.merchant_clean import main as clean_merchant_data


# =====================================================
# PYTHON: LOAD CLEAN DATA → STAGING
# =====================================================

def load_staging_merchant():
    df = pd.read_csv("/clean_data/enterprise/merchant_data.csv")

    insert_cols = [
        "merchant_id",
        "name",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
        "creation_date",
        "merchant_creation_date_key",
    ]

    df = df.loc[:, insert_cols]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.merchant_data;")

    execute_values(
        cur,
        f"""
        INSERT INTO staging.merchant_data (
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
    dag_id="dag_stage_merchant",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "merchant"],
) as dag:

    # -------------------------------------------------
    # 1. RUN CLEANING SCRIPT
    # -------------------------------------------------
    run_cleaning = PythonOperator(
        task_id="clean_merchant_data",
        python_callable=clean_merchant_data,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging_table = PostgresOperator(
        task_id="create_staging_merchant",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.merchant_data (
            merchant_id VARCHAR(30),
            name VARCHAR(255),
            street VARCHAR(255),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date TIMESTAMP,
            merchant_creation_date_key INTEGER
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD CLEAN DATA → STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_merchant",
        python_callable=load_staging_merchant,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    run_cleaning >> create_staging_table >> load_staging
