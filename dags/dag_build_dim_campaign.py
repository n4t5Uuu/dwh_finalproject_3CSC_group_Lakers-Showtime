from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_build_dim_campaign",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimension", "campaign"],
) as dag:

    # -------------------------------------------------
    # 1. CREATE DIM_CAMPAIGN (TYPE 1)
    # -------------------------------------------------
    create_dim_campaign = PostgresOperator(
        task_id="create_dim_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.dim_campaign (
            campaign_key SERIAL PRIMARY KEY,
            campaign_id VARCHAR(30) NOT NULL,
            campaign_name VARCHAR(255),
            campaign_description TEXT,
            discount_pct INTEGER,
            UNIQUE (campaign_id)
        );
        """
    )

    # -------------------------------------------------
    # 2. LOAD DIM_CAMPAIGN (TYPE 1)
    # -------------------------------------------------
    load_dim_campaign = PostgresOperator(
        task_id="load_dim_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.dim_campaign;

        INSERT INTO shopzada.dim_campaign (
            campaign_id,
            campaign_name,
            campaign_description,
            discount_pct
        )
        SELECT
            c.campaign_id,
            c.campaign_name,
            c.campaign_description,
            c.discount_pct
        FROM staging.campaign_data c;
        """
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    create_dim_campaign >> load_dim_campaign
