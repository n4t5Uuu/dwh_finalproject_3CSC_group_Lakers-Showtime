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

def create_staff_table():
    """
    Ensures the dimStaff table exists.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
        staff_key                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        staff_id                 VARCHAR(30) UNIQUE NOT NULL,
        staff_name               VARCHAR(100),
        staff_job_level          VARCHAR(50),
        staff_street             VARCHAR(200),
        staff_state              VARCHAR(100),
        staff_city               VARCHAR(100),
        staff_country            VARCHAR(100),
        staff_contact_number     VARCHAR(50),
        staff_creation_date_key  INT,
        CONSTRAINT fk_staff_creation_date
            FOREIGN KEY (staff_creation_date_key) REFERENCES shopzada.dimDate(date_key)
            ON UPDATE CASCADE ON DELETE SET NULL
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("dimStaff table ensured.")

def load_staff_data():
    """
    Loads staff data from Parquet into dimStaff.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = "/clean_data/enterprise/staff_data.parquet"
    df = pd.read_parquet(file_path)

    # Convert creation_date to YYYYMMDD integer to match dimDate.date_key
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.strftime('%Y%m%d').astype(int)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO shopzada.dimStaff(
                staff_id, staff_name, staff_job_level, staff_street, staff_state,
                staff_city, staff_country, staff_contact_number, staff_creation_date_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (staff_id) DO NOTHING;
        """, (
            row['staff_id'],
            row['name'],
            row.get('job_level'),
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('contact_number'),
            row.get('creation_date')
        ))
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shopzada.dimStaff;")
    print(f"dimStaff loaded with {cursor.fetchone()[0]} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_staff',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dimension', 'staff'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_staff_table',
        python_callable=create_staff_table
    )

    task_load_data = PythonOperator(
        task_id='load_staff_data',
        python_callable=load_staff_data
    )

    task_create_table >> task_load_data
