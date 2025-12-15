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
from business_scripts.business_clean import main as clean_business_product


# =====================================================
# PYTHON: LOAD CLEAN DATA → STAGING
# =====================================================

def load_staging_product_list():
    df = pd.read_csv("/clean_data/business/product_list_clean.csv")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.product_list;")

    execute_values(
        cur,
        """
        INSERT INTO staging.product_list (
            product_id,
            product_name,
            product_type,
            price
        ) VALUES %s
        """,
        df[[
            "product_id",
            "product_name",
            "product_type",
            "price"
        ]].values.tolist()
    )

    conn.commit()
    cur.close()


# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_stage_business_product",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "product"],
) as dag:

    # -------------------------------------------------
    # 1. RUN CLEANING SCRIPT
    # -------------------------------------------------
    run_cleaning = PythonOperator(
        task_id="clean_business_product_data",
        python_callable=clean_business_product,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging_table = PostgresOperator(
        task_id="create_staging_product_list",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.product_list (
            product_id VARCHAR(30),
            product_name VARCHAR(255),
            product_type VARCHAR(100),
            price NUMERIC(10,2)
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD CLEAN DATA → STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_product_list",
        python_callable=load_staging_product_list,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    run_cleaning >> create_staging_table >> load_staging
