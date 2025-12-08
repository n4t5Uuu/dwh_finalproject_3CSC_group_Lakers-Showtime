from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values


# ==========================================================
# PYTHON: Load CSV → STAGING
# ==========================================================
def load_staging_staff():
    df = pd.read_csv("/clean_data/enterprise/staff_data.csv")

    # Convert creation_date string → datetime
    df["creation_date"] = pd.to_datetime(df["creation_date"])

    df_ordered = df[[
        "staff_key",
        "staff_id",
        "name",
        "job_level",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
        "creation_date",
        "staff_creation_date_key"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO staging.staff_data (
            staff_key, staff_id, name, job_level,
            street, state, city, country,
            contact_number, creation_date,
            staff_creation_date_key
        ) VALUES %s
        """,
        df_ordered.values.tolist()
    )

    conn.commit()
    cur.close()



# ==========================================================
# DAG DEFINITION
# ==========================================================
with DAG(
    dag_id="dwh_staff_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staff"],
) as dag:

    # ------------------------------------------------------
    # 1. Create STAGING + DQ tables
    # ------------------------------------------------------
    create_staging = PostgresOperator(
        task_id="create_staging_staff",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS staging.staff_data (
            staff_key VARCHAR(40),
            staff_id VARCHAR(40),
            name VARCHAR(200),
            job_level VARCHAR(50),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date TIMESTAMP,
            staff_creation_date_key INT
        );

        TRUNCATE staging.staff_data;

        CREATE TABLE IF NOT EXISTS shopzada.dq_staff_duplicates (
            staff_id VARCHAR(40),
            duplicate_count INT,
            first_seen TIMESTAMP DEFAULT NOW()
        );

        TRUNCATE shopzada.dq_staff_duplicates;
        """
    )

    # ------------------------------------------------------
    # 2. Load CSV → Staging
    # ------------------------------------------------------
    load_staging_task = PythonOperator(
        task_id="load_staging_staff",
        python_callable=load_staging_staff
    )

    # ------------------------------------------------------
    # 3. Detect duplicates
    # ------------------------------------------------------
    detect_duplicates = PostgresOperator(
        task_id="detect_staff_duplicates",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO shopzada.dq_staff_duplicates (staff_id, duplicate_count)
        SELECT staff_id, COUNT(*)
        FROM staging.staff_data
        GROUP BY staff_id
        HAVING COUNT(*) > 1;
        """
    )

    # ------------------------------------------------------
    # 4. Create dimStaff (if needed)
    # ------------------------------------------------------
    create_dim = PostgresOperator(
        task_id="create_dim_staff",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
            staff_key VARCHAR(40) PRIMARY KEY,
            staff_id VARCHAR(40) NOT NULL,   -- removed UNIQUE!
            name VARCHAR(200),
            job_level VARCHAR(50),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date DATE,
            staff_creation_date_key INT
        );
        """
    )

    # ------------------------------------------------------
    # 5. Load deduped records into dimension
    # ------------------------------------------------------
    load_dim = PostgresOperator(
        task_id="load_dim_staff",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.dimStaff;

        INSERT INTO shopzada.dimStaff (
            staff_key, staff_id, name, job_level,
            street, state, city, country,
            contact_number, creation_date,
            staff_creation_date_key
        )
        SELECT
            staff_key,
            staff_id,
            name,
            job_level,
            street,
            state,
            city,
            country,
            contact_number,
            CAST(creation_date AS DATE),
            staff_creation_date_key
        FROM staging.staff_data;
        """
    )

    # ------------------------------------------------------
    # PIPELINE ORDER
    # ------------------------------------------------------
    create_staging >> load_staging_task >> detect_duplicates >> create_dim >> load_dim
