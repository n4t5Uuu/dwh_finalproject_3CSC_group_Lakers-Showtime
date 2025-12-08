from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =====================================
# PYTHON LOADERS (CSV → staging tables)
# =====================================

def load_staging_user_data():
    df = pd.read_csv("/clean_data/customer_management/user_data.csv")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.user_data;")

    execute_values(
        cur,
        """

        CREATE SCHEMA IF NOT EXISTS staging;

        -- ===============================
        -- STAGING: USER DATA
        -- ===============================
        CREATE TABLE IF NOT EXISTS staging.user_data (
            user_id VARCHAR(30),
            creation_date TIMESTAMP,
            name VARCHAR(200),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            birthdate TIMESTAMP,
            gender VARCHAR(20),
            device_address VARCHAR(50),
            user_type VARCHAR(50),
            creation_date_key INT,
            birth_date_key INT,
            user_id_orig VARCHAR(30),
            user_key VARCHAR(30)
        );

        -- ===============================
        -- STAGING: USER CREDIT CARD
        -- ===============================
        CREATE TABLE IF NOT EXISTS staging.user_credit_card (
            user_key VARCHAR(30),
            name VARCHAR(200),
            credit_card_number VARCHAR(40),
            issuing_bank VARCHAR(50)
        );

        -- ===============================
        -- STAGING: USER JOB
        -- ===============================
        CREATE TABLE IF NOT EXISTS staging.user_job (
            user_key VARCHAR(30),
            name VARCHAR(200),
            job_title VARCHAR(200),
            job_level VARCHAR(200)
        );

        INSERT INTO staging.user_data (
            user_id, creation_date, name, street, state, city, country,
            birthdate, gender, device_address, user_type,
            creation_date_key, birth_date_key, user_id_orig, user_key
        ) VALUES %s

        """,
        df.values.tolist()
    )
    conn.commit()
    cur.close()


def load_staging_user_credit_card():
    df = pd.read_csv("/clean_data/customer_management/user_credit_card.csv")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.user_credit_card;")

    execute_values(
        cur,
        """
        INSERT INTO staging.user_credit_card (
            user_key, name, credit_card_number, issuing_bank
        ) VALUES %s
        """,
        df.values.tolist()
    )
    conn.commit()
    cur.close()


def load_staging_user_job():
    df = pd.read_csv("/clean_data/customer_management/user_job.csv")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.user_job;")

    execute_values(
        cur,
        """
        INSERT INTO staging.user_job (
            user_key, name, job_title, job_level
        ) VALUES %s
        """,
        df.values.tolist()
    )
    conn.commit()
    cur.close()



# ======================
# DAG DEFINITION
# ======================
with DAG(
    dag_id="dwh_user_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "user"],
) as dag:


    # =========================================================
    # 1. CREATE STAGING SCHEMA + STAGING TABLES
    # =========================================================
    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.user_data (
            user_id VARCHAR(30),
            creation_date TIMESTAMP,
            name VARCHAR(200),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            birthdate TIMESTAMP,
            gender VARCHAR(20),
            device_address VARCHAR(50),
            user_type VARCHAR(50),
            creation_date_key INT,
            birth_date_key INT,         -- FIXED: matches CSV
            user_id_orig VARCHAR(30),
            user_key VARCHAR(30)
        );


        CREATE TABLE IF NOT EXISTS staging.user_credit_card (
            user_key VARCHAR(30),
            name VARCHAR(200),
            credit_card_number VARCHAR(40),
            issuing_bank VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS staging.user_job (
            user_key VARCHAR(30),
            name VARCHAR(200),
            job_title VARCHAR(200),
            job_level VARCHAR(200)
        );
        """
    )


    # =========================================================
    # 2. CSV → STAGING TABLES
    # =========================================================
    load_user_data = PythonOperator(
        task_id="load_staging_user_data",
        python_callable=load_staging_user_data,
    )

    load_credit_card = PythonOperator(
        task_id="load_staging_user_credit_card",
        python_callable=load_staging_user_credit_card,
    )

    load_user_job = PythonOperator(
        task_id="load_staging_user_job",
        python_callable=load_staging_user_job,
    )


    # =========================================================
    # 3. BUILD FINAL DIM USER
    # =========================================================
    load_dim_user = PostgresOperator(
        task_id="load_dim_user",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS shopzada.dimUser (
            user_key VARCHAR(30) PRIMARY KEY,
            user_id VARCHAR(30),
            name VARCHAR(200),
            gender VARCHAR(20),
            user_birth_date_key INT,
            user_creation_date_key INT,
            street VARCHAR(200),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            user_type VARCHAR(50),
            device_address VARCHAR(50),
            credit_card_number VARCHAR(40),
            issuing_bank VARCHAR(50),
            job_title VARCHAR(200),
            job_level VARCHAR(200)
        );

        TRUNCATE shopzada.dimUser;

        INSERT INTO shopzada.dimUser (
            user_key, user_id, name, gender,
            user_birth_date_key, user_creation_date_key,
            street, city, state, country,
            user_type, device_address,
            credit_card_number, issuing_bank,
            job_title, job_level
        )
        SELECT
            ud.user_key,
            ud.user_id,
            ud.name,
            ud.gender,
            ud.birth_date_key AS user_birth_date_key,
            ud.creation_date_key AS user_creation_date_key,
            ud.street,
            ud.city,
            ud.state,
            ud.country,
            ud.user_type,
            ud.device_address,
            cc.credit_card_number,
            cc.issuing_bank,
            job.job_title,
            job.job_level
        FROM staging.user_data ud
        LEFT JOIN staging.user_credit_card cc ON ud.user_key = cc.user_key
        LEFT JOIN staging.user_job job ON ud.user_key = job.user_key;

        """
    )


    # ======================
    # DAG DEPENDENCIES
    # ======================
    create_staging_tables >> [load_user_data, load_credit_card, load_user_job] >> load_dim_user
