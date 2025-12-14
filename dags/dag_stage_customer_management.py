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
from customer_management_scripts.customer_management_clean import main as clean_customer_management


# =====================================================
# PYTHON: LOAD CLEAN DATA → STAGING
# =====================================================

def load_staging_user_data_all():
    df = pd.read_csv("/clean_data/customer_management/user_data_all.csv")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.user_data_all;")

    execute_values(
        cur,
        """
        INSERT INTO staging.user_data_all (
            user_id,
            name,
            creation_date,
            birthdate,
            gender,
            street,
            city,
            state,
            country,
            device_address,
            user_type,
            job_title,
            job_level,
            credit_card_number,
            issuing_bank
        ) VALUES %s
        """,
        df[[
            "user_id",
            "name",
            "creation_date",
            "birthdate",
            "gender",
            "street",
            "city",
            "state",
            "country",
            "device_address",
            "user_type",
            "job_title",
            "job_level",
            "credit_card_number",
            "issuing_bank"
        ]].values.tolist()
    )

    conn.commit()
    cur.close()


# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_stage_customer_management",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "customer"],
) as dag:

    # -------------------------------------------------
    # 1. RUN CLEANING SCRIPT
    # -------------------------------------------------
    run_cleaning = PythonOperator(
        task_id="clean_customer_management_data",
        python_callable=clean_customer_management,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging_table = PostgresOperator(
        task_id="create_staging_user_data_all",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.user_data_all (
            user_id VARCHAR(30),
            name VARCHAR(200),
            creation_date TIMESTAMP,
            birthdate TIMESTAMP,
            gender VARCHAR(20),
            street VARCHAR(200),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            device_address VARCHAR(50),
            user_type VARCHAR(50),
            job_title VARCHAR(200),
            job_level VARCHAR(200),
            credit_card_number VARCHAR(40),
            issuing_bank VARCHAR(50)
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD CLEAN DATA → STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_user_data_all",
        python_callable=load_staging_user_data_all,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    run_cleaning >> create_staging_table >> load_staging
