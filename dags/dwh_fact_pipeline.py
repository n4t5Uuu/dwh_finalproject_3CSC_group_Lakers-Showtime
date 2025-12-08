from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values

# ============================================================
# PYTHON STAGING LOADERS
# ============================================================

def load_staging_lineitem():
    df = pd.read_csv("/clean_data/facts/factLineItem.csv")

    # Compute line_amount
    df["line_amount"] = df["price_per_quantity"] * df["quantity"]

    # Ensure correct column order to match staging table
    df = df[[
        "order_id",
        "user_key",
        "merchant_key",
        "staff_key",
        "product_id",
        "product_name",
        "price_per_quantity",
        "quantity",
        "line_amount",
        "date_key"    # already computed during your cleaning
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO staging.lineitem (
            order_id, user_key, merchant_key, staff_key,
            product_id, product_name,
            price_per_quantity, quantity, line_amount, date_key
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()



def load_staging_orders():
    df = pd.read_csv("/clean_data/operations/orders_final_ready.csv")

    df = df.rename(columns={
        "transaction_date_key": "date_key"
    })

    df = df[[
        "order_id", "user_key", "merchant_key",
        "staff_key", "date_key",
        "estimated_arrival_in_days", "delay_in_days"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(cur, """
        INSERT INTO staging.orders (
            order_id, user_key, merchant_key, staff_key,
            date_key, estimated_arrival_in_days, delay_in_days
        ) VALUES %s
    """, df.values.tolist())

    conn.commit()
    cur.close()


def load_staging_campaign():
    df = pd.read_csv("/clean_data/marketing/transactional_campaign_data.csv")

    df["availed"] = df["availed"].astype(int)
    df = df.rename(columns={"transaction_date_key": "date_key"})

    df = df[[
        "order_id", "campaign_id", "availed",
        "date_key", "estimated_arrival_days"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(cur, """
        INSERT INTO staging.campaign_txn (
            order_id, campaign_id, availed,
            date_key, estimated_arrival_days
        ) VALUES %s
    """, df.values.tolist())

    conn.commit()
    cur.close()


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="dwh_fact_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "fact"],
) as dag:

    # -------------------------------
    # STAGING TABLES
    # -------------------------------
    create_staging = PostgresOperator(
        task_id="create_staging",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.lineitem (
            order_id VARCHAR(100),
            user_key VARCHAR(30),
            merchant_key VARCHAR(40),
            staff_key VARCHAR(40),
            product_id VARCHAR(50),
            product_name VARCHAR(200),
            price_per_quantity NUMERIC(12,2),
            quantity INT,
            line_amount NUMERIC(14,2),
            date_key INT
        );


        CREATE TABLE IF NOT EXISTS staging.orders (
            order_id VARCHAR(100),
            user_key VARCHAR(30),
            merchant_key VARCHAR(40),
            staff_key VARCHAR(40),
            date_key INT,
            estimated_arrival_in_days INT,
            delay_in_days INT
        );

        CREATE TABLE IF NOT EXISTS staging.campaign_txn (
            order_id VARCHAR(100),
            campaign_id VARCHAR(40),
            availed INT,
            date_key INT,
            estimated_arrival_days INT
        );

        TRUNCATE staging.lineitem;
        TRUNCATE staging.orders;
        TRUNCATE staging.campaign_txn;
        """
    )

    load_lineitem = PythonOperator(
        task_id="load_staging_lineitem",
        python_callable=load_staging_lineitem
    )

    load_orders = PythonOperator(
        task_id="load_staging_orders",
        python_callable=load_staging_orders
    )

    load_campaign = PythonOperator(
        task_id="load_staging_campaign",
        python_callable=load_staging_campaign
    )

    # -------------------------------
    # LOAD factLineItem
    # -------------------------------
    load_fact_lineitem = PostgresOperator(
        task_id="load_fact_lineitem",
        postgres_conn_id="postgres_default",
        sql="""
        DELETE FROM shopzada.factLineItem;

        INSERT INTO shopzada.factLineItem (
            order_id, user_key, merchant_key, staff_key,
            product_id, price_per_quantity,
            quantity, line_amount, date_key
        )
        SELECT
            order_id, user_key, merchant_key, staff_key,
            product_id, price_per_quantity,
            quantity, (price_per_quantity * quantity) AS line_amount,
            date_key
        FROM staging.lineitem;

        """
    )

    # -------------------------------
    # LOAD factOrders
    # -------------------------------
    load_fact_orders = PostgresOperator(
        task_id="load_fact_orders",
        postgres_conn_id="postgres_default",
        sql="""
        DELETE FROM shopzada.factOrders;

        INSERT INTO shopzada.factOrders (
            order_id, user_key, merchant_key, staff_key,
            date_key, transaction_amount,
            estimated_arrival_in_days, delay_in_days
        )
        SELECT
            o.order_id,
            o.user_key,
            o.merchant_key,
            o.staff_key,
            o.date_key,
            COALESCE(SUM(li.line_amount), 0) AS transaction_amount,
            o.estimated_arrival_in_days,
            o.delay_in_days
        FROM staging.orders o
        LEFT JOIN shopzada.factLineItem li
            ON o.order_id = li.order_id
        GROUP BY
            o.order_id, o.user_key, o.merchant_key, o.staff_key,
            o.date_key, o.estimated_arrival_in_days, o.delay_in_days;
        """
    )

    # -------------------------------
    # LOAD factCampaignAvailed
    # -------------------------------
    load_fact_campaign = PostgresOperator(
        task_id="load_fact_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        DELETE FROM shopzada.factCampaignAvailed;

        INSERT INTO shopzada.factCampaignAvailed (
            order_id, campaign_key, user_key, merchant_key,
            staff_key, date_key, availed, discount_pct
        )
        SELECT
            c.order_id,
            d.campaign_key,
            o.user_key,
            o.merchant_key,
            o.staff_key,
            c.date_key,
            c.availed,
            d.discount_pct
        FROM staging.campaign_txn c
        LEFT JOIN shopzada.dimCampaign d
            ON c.campaign_id = d.campaign_id
        LEFT JOIN staging.orders o
            ON c.order_id = o.order_id;
        """
    )

    create_staging >> [load_lineitem, load_orders, load_campaign] >> load_fact_lineitem >> load_fact_orders >> load_fact_campaign
