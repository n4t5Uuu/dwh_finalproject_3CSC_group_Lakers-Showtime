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

def create_user_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shopzada.dimUser (
        user_key                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id                 VARCHAR(30) UNIQUE NOT NULL,
        user_creation_date_key  INT,
        user_birth_date_key     INT,
        CONSTRAINT fk_user_creation_date FOREIGN KEY (user_creation_date_key)
            REFERENCES shopzada.dimDate(date_key) ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_user_birth_date    FOREIGN KEY (user_birth_date_key)
            REFERENCES shopzada.dimDate(date_key) ON UPDATE CASCADE ON DELETE SET NULL,
        user_name               VARCHAR(100),
        user_street             VARCHAR(200),
        user_state              VARCHAR(100),
        user_city               VARCHAR(100),
        user_country            VARCHAR(100),
        user_gender             VARCHAR(20),
        user_device_address     VARCHAR(50),
        user_user_type          VARCHAR(50),
        user_issuing_bank       VARCHAR(100),
        user_credit_card        VARCHAR(32),
        user_job_title          VARCHAR(100),
        user_job_level          VARCHAR(50)
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("dimUser table ensured.")

def load_user_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Load the three parquet files
    df_user = pd.read_parquet("/clean_data/customer/user_data.parquet")
    df_card = pd.read_parquet("/clean_data/customer/user_credit_card.parquet")
    df_job = pd.read_csv("/clean_data/customer/user_job_clean.csv")

    # Merge on user_id
    df = df_user.merge(df_card[['user_id','credit_card_number','issuing_bank']], on='user_id', how='left')
    df = df.merge(df_job[['user_id','job_title','job_level']], on='user_id', how='left')

    # Convert dates to YYYYMMDD integers for dimDate
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.strftime('%Y%m%d').astype(int)
    df['birthdate'] = pd.to_datetime(df['birthdate']).dt.strftime('%Y%m%d').astype(int)

    # Prepare values for batch insert
    values = [
        (
            row['user_id'],
            row['creation_date'],
            row['birthdate'],
            row['name'],
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('gender'),
            row.get('device_address'),
            row.get('user_type'),
            row.get('issuing_bank'),
            row.get('credit_card_number'),
            row.get('job_title'),
            row.get('job_level')
        )
        for _, row in df.iterrows()
    ]

    # Batch insert with ON CONFLICT
    execute_values(cursor, """
        INSERT INTO shopzada.dimUser(
            user_id, user_creation_date_key, user_birth_date_key, user_name,
            user_street, user_state, user_city, user_country,
            user_gender, user_device_address, user_user_type,
            user_issuing_bank, user_credit_card, user_job_title, user_job_level
        )
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING;
    """, values)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shopzada.dimUser;")
    print(f"dimUser loaded with {cursor.fetchone()[0]} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_user',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'user'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_user_table',
        python_callable=create_user_table
    )

    task_load_data = PythonOperator(
        task_id='load_user_data',
        python_callable=load_user_data
    )

    task_create_table >> task_load_data
