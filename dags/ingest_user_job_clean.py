from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 🔧 CONFIG
# ============================================================

DATA_DIR = "/clean_data/customer_management"          # edit path if needed
USER_JOB_FILE_NAME = "user_job.csv"
USER_JOB_CSV_PATH = f"{DATA_DIR}/{USER_JOB_FILE_NAME}"

POSTGRES_CONN_ID = "postgres_default"

RAW_SCHEMA = "shopzada"
RAW_TABLE = "user_job_raw"
# ============================================================


def create_raw_table():
    """
    Create schema + raw table for user_job if not exists.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
        user_key        TEXT,
        name            TEXT,
        job_title       TEXT,
        job_level       TEXT,
        load_timestamp  TIMESTAMPTZ DEFAULT NOW(),
        source_file     TEXT
    );
    """

    pg_hook.run(create_sql)


def load_user_job():
    """
    Load user_job.csv into RAW table without cleaning.
    """
    df = pd.read_csv(USER_JOB_CSV_PATH)

    df["source_file"] = USER_JOB_FILE_NAME

    rows = list(df.itertuples(index=False, name=None))

    insert_sql = f"""
        INSERT INTO {RAW_SCHEMA}.{RAW_TABLE}
        (
            user_key,
            name,
            job_title,
            job_level,
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
    "retries": 1,
}

with DAG(
    dag_id="ingest_user_job_raw",
    default_args=default_args,
    description="Ingest user_job.csv into RAW user_job table",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["shopzada", "raw", "ingest", "business"],
) as dag:

    create_raw_table_task = PythonOperator(
        task_id="create_user_job_raw_table",
        python_callable=create_raw_table,
    )

    load_user_job_task = PythonOperator(
        task_id="load_user_job",
        python_callable=load_user_job,
    )

    create_raw_table_task >> load_user_job_task
