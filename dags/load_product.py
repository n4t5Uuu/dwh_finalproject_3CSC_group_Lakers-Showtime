from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def create_product_table():
    """
    Ensures dimProduct table exists.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shopzada.dimProduct (
        product_key     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_id      VARCHAR(30) UNIQUE NOT NULL,
        product_name    VARCHAR(200),
        product_type    VARCHAR(100),
        product_price   NUMERIC(12,2)
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("dimProduct table ensured.")

def load_product_data():
    """
    Loads product data from Parquet into dimProduct.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = "/ingested/business/product_list.parquet"
    df = pd.read_parquet(file_path)

    # Prepare values for batch insert
    values = [
        (
            row['product_id'],
            row.get('product_name'),
            row.get('product_type'),
            row.get('price')
        )
        for _, row in df.iterrows()
    ]

    # Batch insert with ON CONFLICT to avoid duplicates
    execute_values(cursor, """
        INSERT INTO shopzada.dimProduct(
            product_id, product_name, product_type, product_price
        )
        VALUES %s
        ON CONFLICT (product_id) DO NOTHING;
    """, values)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shopzada.dimProduct;")
    print(f"dimProduct loaded with {cursor.fetchone()[0]} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_product',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'product'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_product_table',
        python_callable=create_product_table
    )

    task_load_data = PythonOperator(
        task_id='load_product_data',
        python_callable=load_product_data
    )

    task_create_table >> task_load_data
