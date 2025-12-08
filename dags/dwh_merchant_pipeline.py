from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values


# ============================================
# LOAD CSV → STAGING (Python)
# ============================================
def load_staging_merchant():
    df = pd.read_csv("/clean_data/enterprise/merchant_data.csv")

    # Ensure correct type for creation_date
    df["creation_date"] = pd.to_datetime(df["creation_date"])

    # Enforce correct column order
    df_ordered = df[[
        "merchant_key",
        "merchant_id",
        "name",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
        "creation_date",
        "merchant_creation_date_key"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO staging.merchant_data (
            merchant_key, merchant_id, name,
            street, state, city, country,
            contact_number, creation_date, merchant_creation_date_key
        )
        VALUES %s
        """,
        df_ordered.values.tolist()
    )

    conn.commit()
    cur.close()



# ===========================================================
# DAG
# ===========================================================
with DAG(
    dag_id="dwh_merchant_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "merchant"],
) as dag:


    # -------------------------------------------------------
    # 1. CREATE STAGING + DQ TABLES
    # -------------------------------------------------------
    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS shopzada;

        -- STAGING
        CREATE TABLE IF NOT EXISTS staging.merchant_data (
            merchant_key VARCHAR(40),
            merchant_id VARCHAR(40),
            name VARCHAR(200),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date TIMESTAMP,
            merchant_creation_date_key INT
        );

        TRUNCATE staging.merchant_data;

        -- DATA QUALITY TABLE (duplicates)
        CREATE TABLE IF NOT EXISTS shopzada.dq_merchant_duplicates (
            merchant_id VARCHAR(40),
            duplicate_count INT,
            first_seen TIMESTAMP DEFAULT NOW()
        );
        """
    )


    # -------------------------------------------------------
    # 2. LOAD CSV → STAGING
    # -------------------------------------------------------
    load_to_staging = PythonOperator(
        task_id="load_staging_merchant",
        python_callable=load_staging_merchant
    )


    # -------------------------------------------------------
    # 3. DUPLICATE DETECTION
    # -------------------------------------------------------
    detect_duplicates = PostgresOperator(
        task_id="detect_duplicates",
        postgres_conn_id="postgres_default",
        sql="""
        -- Clear old DQ entries
        TRUNCATE shopzada.dq_merchant_duplicates;

        -- Insert duplicate merchant_ids from staging
        INSERT INTO shopzada.dq_merchant_duplicates (merchant_id, duplicate_count)
        SELECT merchant_id, COUNT(*)
        FROM staging.merchant_data
        GROUP BY merchant_id
        HAVING COUNT(*) > 1;
        """
    )


    # -------------------------------------------------------
    # 4. CREATE DIM TABLE (if missing)
    # -------------------------------------------------------
    create_dim_table = PostgresOperator(
        task_id="create_dim_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
            merchant_key VARCHAR(40) PRIMARY KEY,
            merchant_id VARCHAR(40) NOT NULL,
            name VARCHAR(200),
            street VARCHAR(200),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date DATE,
            merchant_creation_date_key INT
        );

        """
    )


    # -------------------------------------------------------
    # 5. LOAD DEDUPED MERCHANTS INTO dimMerchant
    # -------------------------------------------------------
    load_dim_table = PostgresOperator(
        task_id="load_dim_merchant",
        postgres_conn_id="postgres_default",
        sql="""
        -- Remove old data
        TRUNCATE shopzada.dimMerchant;

        -- Insert deduped merchants:
        -- rule: KEEP THE ROW WITH THE NEWEST creation_date
        TRUNCATE shopzada.dimMerchant;

        INSERT INTO shopzada.dimMerchant (
            merchant_key, merchant_id, name,
            street, state, city, country,
            contact_number, creation_date,
            merchant_creation_date_key
        )
        SELECT
            merchant_key, merchant_id, name,
            street, state, city, country,
            contact_number,
            CAST(creation_date AS DATE),
            merchant_creation_date_key
        FROM staging.merchant_data
        ORDER BY merchant_id, creation_date DESC;
        """
    )


    # =======================================================
    # PIPELINE ORDER
    # =======================================================
    create_staging_tables >> load_to_staging >> detect_duplicates >> create_dim_table >> load_dim_table
