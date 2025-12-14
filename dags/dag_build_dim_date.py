from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="dag_build_dim_date",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimension", "date"],
) as dag:

    dag_build_dim_date = PostgresOperator(
        task_id="dag_build_dim_date",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            day INT,
            day_name VARCHAR(10),
            month INT,
            month_name VARCHAR(10),
            quarter INT,
            quarter_name VARCHAR(6),
            year INT,
            is_weekend BOOLEAN
        );

        TRUNCATE shopzada.dim_date;

        INSERT INTO shopzada.dim_date
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INT        AS date_key,
            d                                 AS full_date,
            EXTRACT(DAY FROM d)::INT          AS day,
            TO_CHAR(d, 'Day')                 AS day_name,
            EXTRACT(MONTH FROM d)::INT        AS month,
            TO_CHAR(d, 'Month')               AS month_name,
            EXTRACT(QUARTER FROM d)::INT      AS quarter,
            'Q' || EXTRACT(QUARTER FROM d)    AS quarter_name,
            EXTRACT(YEAR FROM d)::INT         AS year,
            CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7)
                 THEN TRUE ELSE FALSE END     AS is_weekend
        FROM generate_series(
            DATE '1970-01-01',
            DATE '2030-12-31',
            INTERVAL '1 day'
        ) AS d;

        """
    )
