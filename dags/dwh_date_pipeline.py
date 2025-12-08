from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values

# ==========================================================
# LOAD CSV INTO dimdate
# ==========================================================
def load_dim_date():
    df = pd.read_csv("/clean_data/dimensions/dimDate.csv")

    # Reorder columns exactly as table definition
    df = df[[
        "date_key", "date_full", "date_day", "date_day_name",
        "date_month", "date_month_name",
        "date_quarter", "date_quarter_name",
        "date_half_year", "date_half_year_name",
        "date_year", "date_is_weekend", "date_is_holiday"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO shopzada.dimdate (
            date_key, date_full, date_day, date_day_name,
            date_month, date_month_name,
            date_quarter, date_quarter_name,
            date_half_year, date_half_year_name,
            date_year, date_is_weekend, date_is_holiday
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()


# ==========================================================
# DAG DEFINITION
# ==========================================================
with DAG(
    dag_id="dwh_dim_date_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dim", "date", "dwh"],
) as dag:

    # Ensure schema + recreate table
    recreate_table = PostgresOperator(
        task_id="recreate_dimdate_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        DROP TABLE IF EXISTS shopzada.dimdate;

        CREATE TABLE shopzada.dimdate (
            date_key            INT PRIMARY KEY,
            date_full           DATE,
            date_day            INT,
            date_day_name       VARCHAR(20),
            date_month          INT,
            date_month_name     VARCHAR(20),
            date_quarter        INT,
            date_quarter_name   VARCHAR(10),
            date_half_year      INT,
            date_half_year_name VARCHAR(10),
            date_year           INT,
            date_is_weekend     BOOLEAN,
            date_is_holiday     BOOLEAN
        );
        """
    )

    # Load CSV into the table
    load_dim = PythonOperator(
        task_id="load_dimdate",
        python_callable=load_dim_date
    )

    recreate_table >> load_dim
