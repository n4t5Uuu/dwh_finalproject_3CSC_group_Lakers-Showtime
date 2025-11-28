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

def create_merchant_table():
    """
    Ensures the dimMerchant table exists.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
        merchant_key               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        merchant_id                VARCHAR(30) UNIQUE NOT NULL,
        merchant_name              VARCHAR(100),
        merchant_street            VARCHAR(200),
        merchant_state             VARCHAR(200),
        merchant_city              VARCHAR(100),
        merchant_country           VARCHAR(100),
        merchant_contact_number    VARCHAR(50),
        merchant_creation_date_key INT,
        CONSTRAINT fk_merchant_creation_date
            FOREIGN KEY (merchant_creation_date_key) REFERENCES shopzada.dimDate(date_key)
            ON UPDATE CASCADE ON DELETE SET NULL
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("dimMerchant table ensured.")

def load_merchant_data():
    """
    Loads merchant data from Parquet into dimMerchant.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = "/clean_data/enterprise/merchant_data.parquet"
    df = pd.read_parquet(file_path)

    # Convert creation_date to YYYYMMDD integer to match dimDate.date_key
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.strftime('%Y%m%d').astype(int)

    # Insert data row by row with ON CONFLICT DO NOTHING
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO shopzada.dimMerchant(
                merchant_id, merchant_name, merchant_street, merchant_state,
                merchant_city, merchant_country, merchant_contact_number,
                merchant_creation_date_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (merchant_id) DO NOTHING;
        """, (
            row['merchant_id'],
            row['name'],
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('contact_number'),
            row.get('creation_date')
        ))
    conn.commit()

    # Optional: show how many rows in table
    cursor.execute("SELECT COUNT(*) FROM shopzada.dimMerchant;")
    print(f"dimMerchant loaded with {cursor.fetchone()[0]} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_merchant',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'merchant'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_merchant_table',
        python_callable=create_merchant_table
    )

    task_load_data = PythonOperator(
        task_id='load_merchant_data',
        python_callable=load_merchant_data
    )

    task_create_table >> task_load_data
