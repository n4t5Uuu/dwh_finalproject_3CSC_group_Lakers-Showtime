from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values


# ================================
# 1. LOAD CSV → STAGING
# ================================
def load_staging_campaign():
    df = pd.read_csv("/clean_data/marketing/campaign_data.csv")

    df_ordered = df[[
        "campaign_key",
        "campaign_id",
        "campaign_name",
        "campaign_description",
        "discount_pct"
    ]]

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO staging.campaign_data (
            campaign_key,
            campaign_id,
            campaign_name,
            campaign_description,
            discount_pct
        ) VALUES %s
        """,
        df_ordered.values.tolist()
    )

    conn.commit()
    cur.close()



# ==========================================================
# DAG
# ==========================================================
with DAG(
    dag_id="dwh_campaign_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "campaign"],
) as dag:

    # 1. CREATE STAGING
    create_staging = PostgresOperator(
        task_id="create_staging_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.campaign_data (
            campaign_key VARCHAR(40),
            campaign_id VARCHAR(40),
            campaign_name VARCHAR(200),
            campaign_description TEXT,
            discount_pct INT
        );

        TRUNCATE staging.campaign_data;
        """
    )

    # 2. LOAD STAGING
    load_staging = PythonOperator(
        task_id="load_staging_campaign",
        python_callable=load_staging_campaign
    )

    # 3. CREATE DIM
    create_dim = PostgresOperator(
        task_id="create_dim_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
            campaign_key VARCHAR(40) PRIMARY KEY,
            campaign_id  VARCHAR(40) NOT NULL,
            campaign_name VARCHAR(200),
            campaign_description TEXT,
            discount_pct INT
        );
        """
    )

    # 4. LOAD DIM
    load_dim = PostgresOperator(
        task_id="load_dim_campaign",
        postgres_conn_id="postgres_default",
        sql="""
        DELETE FROM shopzada.dimCampaign;

        INSERT INTO shopzada.dimCampaign (
            campaign_key,
            campaign_id,
            campaign_name,
            campaign_description,
            discount_pct
        )
        SELECT
            campaign_key,
            campaign_id,
            campaign_name,
            campaign_description,
            discount_pct
        FROM staging.campaign_data;
        """
    )

    create_staging >> load_staging >> create_dim >> load_dim
