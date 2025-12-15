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
from enterprise_scripts.staff_clean import main as clean_staff_data


# =====================================================
# PYTHON: LOAD CLEAN DATA → STAGING
# =====================================================

def load_staging_staff():
    df = pd.read_csv("/clean_data/enterprise/staff_data.csv")

    # 🔒 HARD LOCK columns (order + count)
    insert_cols = [
        "staff_id",
        "name",
        "job_level",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
        "creation_date",
        "staff_creation_date_key",
    ]

    df = df.loc[:, insert_cols]

    # Optional sanity check (remove after debugging)
    assert df.shape[1] == len(insert_cols), f"Column mismatch: {df.columns.tolist()}"

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("TRUNCATE staging.staff_data;")

    execute_values(
        cur,
        f"""
        INSERT INTO staging.staff_data (
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
    dag_id="dag_stage_staff",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "staff"],
) as dag:

    # -------------------------------------------------
    # 1. RUN CLEANING SCRIPT
    # -------------------------------------------------
    run_cleaning = PythonOperator(
        task_id="clean_staff_data",
        python_callable=clean_staff_data,
    )

    # -------------------------------------------------
    # 2. CREATE STAGING TABLE
    # -------------------------------------------------
    create_staging_table = PostgresOperator(
        task_id="create_staging_staff",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.staff_data (
            staff_id VARCHAR(30),
            name VARCHAR(255),
            job_level VARCHAR(50),
            street VARCHAR(255),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),
            creation_date TIMESTAMP,
            staff_creation_date_key INTEGER
        );
        """
    )

    # -------------------------------------------------
    # 3. LOAD CLEAN DATA → STAGING
    # -------------------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_staff",
        python_callable=load_staging_staff,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    run_cleaning >> create_staging_table >> load_staging
