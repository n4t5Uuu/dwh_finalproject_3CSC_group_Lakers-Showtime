from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="dag_build_fact_campaign_availed",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "fact", "campaign"],
) as dag:

    # -----------------------------------
    # 1. CREATE FACT TABLE
    # -----------------------------------
    create_fact_campaign = PostgresOperator(
        task_id="create_fact_campaign_availed",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.fact_campaign_availed (
            fact_campaign_key SERIAL PRIMARY KEY,

            order_id VARCHAR(100) NOT NULL,

            campaign_key INT NOT NULL,
            user_key INT NOT NULL,
            merchant_key INT NOT NULL,
            staff_key INT NOT NULL,
            date_key INT NOT NULL,

            availed INT NOT NULL,
            discount_pct INT,

            CONSTRAINT uq_fact_campaign UNIQUE (order_id, campaign_key, date_key)
        );
        """
    )

    # -----------------------------------
    # 2. LOAD FACT TABLE
    # -----------------------------------
    load_fact_campaign = PostgresOperator(
        task_id="load_fact_campaign_availed",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.fact_campaign_availed;

        INSERT INTO shopzada.fact_campaign_availed (
            order_id,
            campaign_key,
            user_key,
            merchant_key,
            staff_key,
            date_key,
            availed,
            discount_pct
        )
        SELECT
            tc.order_id,
            dc.campaign_key,
            COALESCE(du.user_key, 0)      AS user_key,
            COALESCE(dm.merchant_key, 0)  AS merchant_key,
            COALESCE(ds.staff_key, 0)     AS staff_key,
            tc.date_key,
            tc.availed,
            dc.discount_pct
        FROM staging.transactional_campaign_clean tc

        -- CAMPAIGN DIM (Type 1)
        LEFT JOIN shopzada.dim_campaign dc
            ON tc.campaign_id = dc.campaign_id

        -- ORDER → USER
        LEFT JOIN staging.orders_clean o
            ON tc.order_id = o.order_id

        LEFT JOIN shopzada.dim_user du
            ON o.user_id = du.user_id
        AND TO_DATE(tc.date_key::text, 'YYYYMMDD')
            BETWEEN du.effective_from
                AND COALESCE(du.effective_to, DATE '9999-12-31')


        -- ORDER → MERCHANT / STAFF
        LEFT JOIN staging.order_with_merchant_clean om
            ON tc.order_id = om.order_id

        LEFT JOIN shopzada.dim_merchant dm
        ON om.merchant_id = dm.merchant_id
        AND TO_DATE(tc.date_key::text, 'YYYYMMDD')
            BETWEEN dm.effective_from
                AND COALESCE(dm.effective_to, DATE '9999-12-31')

        LEFT JOIN shopzada.dim_staff ds
        ON om.staff_id = ds.staff_id
        AND TO_DATE(tc.date_key::text, 'YYYYMMDD')
            BETWEEN ds.effective_from
                AND COALESCE(ds.effective_to, DATE '9999-12-31')




        """
    )

    # -----------------------------------
    # DAG ORDER
    # -----------------------------------
    create_fact_campaign >> load_fact_campaign
