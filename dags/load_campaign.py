from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def create_campaign_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
        campaign_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        campaign_id           VARCHAR(30) UNIQUE NOT NULL,
        campaign_name         VARCHAR(100),
        campaign_description  TEXT,
        campaign_discount     NUMERIC(6,2)
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("dimCampaign table ensured.")

def load_campaign_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    campaign_file = "/clean_data/marketing/campaign_data.parquet"
    df = pd.read_parquet(campaign_file)

    # Insert data
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO shopzada.dimCampaign(campaign_id, campaign_name, campaign_description, campaign_discount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (campaign_id) DO NOTHING;
        """, (row['campaign_id'], row['campaign_name'], row.get('campaign_description'), row.get('discount')))
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shopzada.dimCampaign;")
    print(f"dimCampaign loaded with {cursor.fetchone()[0]} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_campaign',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'campaign'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_campaign_table',
        python_callable=create_campaign_table
    )

    task_load_data = PythonOperator(
        task_id='load_campaign_data',
        python_callable=load_campaign_data
    )

    task_create_table >> task_load_data
