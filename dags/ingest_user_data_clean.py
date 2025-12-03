from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 🔧 CONFIGURATION VARIABLES
# ============================================================

# Path INSIDE Airflow container (you will edit this)
DATA_DIR = "/clean_data/customer_management"

# Name of your CSV file (edit if different)
USER_FILE_NAME = "user_data.csv"

# Full path (auto-constructed)
USER_CSV_PATH = f"{DATA_DIR}/{USER_FILE_NAME}"

# Postgres Airflow connection ID
POSTGRES_CONN_ID = "postgres_default"

# Schema and table for RAW layer
RAW_SCHEMA = "shopzada"
RAW_TABLE = "user_list_raw"
# ============================================================


def create_raw_table():
    """
    Create schema + raw table for user_list if not exists.
    All columns from CSV are TEXT; metadata columns included.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
        user_id         TEXT,
        creation_date   TEXT,
        name            TEXT,
        street          TEXT,
        state           TEXT,
        city            TEXT,
        country         TEXT,
        birthdate       TEXT,
        gender          TEXT,
        device_address  TEXT,
        user_type       TEXT,
        user_id_orig    TEXT,
        user_key        TEXT,
        load_timestamp  TIMESTAMPTZ DEFAULT NOW(),
        source_file     TEXT
    );
    """

    pg_hook.run(create_sql)


def load_user_list():
    """
    Load user_data.csv into RAW table without cleaning.
    """
    # Load CSV as-is
    df = pd.read_csv(USER_CSV_PATH)

    # Add metadata
    df["source_file"] = USER_FILE_NAME

    # Convert DataFrame rows into list-of-tuples for fast insert
    rows = list(df.itertuples(index=False, name=None))

    insert_sql = f"""
        INSERT INTO {RAW_SCHEMA}.{RAW_TABLE}
        (
            user_id,
            creation_date,
            name,
            street,
            state,
            city,
            country,
            birthdate,
            gender,
            device_address,
            user_type,
            user_id_orig,
            user_key,
            source_file
        )
        VALUES %s;
    """

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    execute_values(cur, insert_sql, rows)
    conn.commit()

    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="ingest_user_data_clean",
    default_args=default_args,
    description="Ingest user_data.csv into RAW user table",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["shopzada", "raw", "ingest", "business"],
) as dag:

    create_raw_table_task = PythonOperator(
        task_id="create_user_raw_table",
        python_callable=create_raw_table,
    )

    load_user_list_task = PythonOperator(
        task_id="load_user_list",
        python_callable=load_user_list,
    )

    create_raw_table_task >> load_user_list_task
