from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================

# Path INSIDE Airflow container (mapped from ./data_files/)
DATA_DIR = "/clean_data/business"

# Name of your CSV file
PRODUCT_FILE_NAME = "product_list.csv"

# Full path (auto-constructed)
PRODUCT_CSV_PATH = f"{DATA_DIR}/{PRODUCT_FILE_NAME}"

# Postgres Airflow connection ID
POSTGRES_CONN_ID = "postgres_default"

# Schema and table for RAW layer
RAW_SCHEMA = "shopzada"
RAW_TABLE = "product_list_raw"
# ============================================================



def create_raw_table():
    """
    Create schema + raw table for product_list if not exists.
    All columns from CSV are TEXT; metadata columns included.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
        product_id     TEXT,
        product_name   TEXT,
        product_type   TEXT,
        price          TEXT,
        product_key    TEXT,
        load_timestamp TIMESTAMPTZ DEFAULT NOW(),
        source_file    TEXT
    );
    """

    pg_hook.run(create_sql)



def load_product_list():
    """
    Load product_list.csv into RAW table without cleaning.
    """
    # Load CSV as-is
    df = pd.read_csv(PRODUCT_CSV_PATH)

    # Add metadata
    df["source_file"] = PRODUCT_FILE_NAME

    # Convert DataFrame rows into list-of-tuples for fast insert
    rows = list(df.itertuples(index=False, name=None))

    insert_sql = f"""
        INSERT INTO {RAW_SCHEMA}.{RAW_TABLE}
        (product_id, product_name, product_type, price, product_key, source_file)
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
    "retries": 1,
}

with DAG(
    dag_id="ingest_product_list_raw",
    default_args=default_args,
    description="Ingest product_list.csv into RAW product table",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["shopzada", "raw", "ingest", "business"],
) as dag:

    create_raw_table_task = PythonOperator(
        task_id="create_raw_table",
        python_callable=create_raw_table,
    )

    load_product_list_task = PythonOperator(
        task_id="load_product_list",
        python_callable=load_product_list,
    )

    create_raw_table_task >> load_product_list_task
