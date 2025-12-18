from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="dag_build_fact_orders",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "fact", "orders"],
) as dag:


    create_fact_orders = PostgresOperator(
        task_id="create_fact_orders",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.fact_orders (
            fact_orders_key BIGSERIAL PRIMARY KEY,

            order_id VARCHAR(100) NOT NULL,

            user_key INT NOT NULL,
            merchant_key INT NOT NULL DEFAULT 0,
            staff_key INT NOT NULL DEFAULT 0,
            date_key INT NOT NULL,

            order_amount NUMERIC(14,2) NOT NULL,
            estimated_arrival_days INT,
            delay_in_days INT
        );
        """
    )


    load_fact_orders = PostgresOperator(
        task_id="load_fact_orders",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.fact_orders;

        INSERT INTO shopzada.fact_orders (
            order_id,
            user_key,
            merchant_key,
            staff_key,
            date_key,
            order_amount,
            estimated_arrival_days,
            delay_in_days
        )
        SELECT
            li.order_id,

            li.user_key,
            li.merchant_key,
            li.staff_key,
            li.date_key,

            SUM(li.line_amount) AS order_amount,

            o.estimated_arrival_days,
            d.delay_in_days

        FROM shopzada.fact_line_item li

        LEFT JOIN staging.orders_clean o
            ON li.order_id = o.order_id

        LEFT JOIN staging.order_delays_clean d
            ON li.order_id = d.order_id

        GROUP BY
            li.order_id,
            li.user_key,
            li.merchant_key,
            li.staff_key,
            li.date_key,
            o.estimated_arrival_days,
            d.delay_in_days;
        """
    )

    create_fact_orders >> load_fact_orders
