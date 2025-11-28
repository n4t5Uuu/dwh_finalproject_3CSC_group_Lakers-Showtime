from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, date
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dim_date_table():
    """Create shopzada schema and dimDate table if not exists"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_sql = """
    CREATE SCHEMA IF NOT EXISTS shopzada;

    CREATE TABLE IF NOT EXISTS shopzada.dimDate (
        date_key            BIGINT PRIMARY KEY,
        date_full           DATE NOT NULL,
        date_day            INT NOT NULL,
        date_day_name       VARCHAR(20),
        date_month          INT NOT NULL,
        date_month_name     VARCHAR(20),
        date_quarter        INT,
        date_quarter_name   VARCHAR(20),
        date_half_year      INT,
        date_half_year_name VARCHAR(20),
        date_year           INT NOT NULL,
        date_is_weekend     BOOLEAN DEFAULT FALSE,
        date_is_holiday     BOOLEAN DEFAULT FALSE
    );
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("Schema and dimDate table ensured.")

def load_dim_date():
    """Populate dimDate from 1950 to 2030"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    start_date = date(1950, 1, 1)
    end_date = date(2030, 12, 31)
    delta = timedelta(days=1)

    rows = []
    current = start_date
    while current <= end_date:
        date_key = int(current.strftime('%Y%m%d'))
        rows.append((
            date_key,
            current,
            current.day,
            current.strftime('%A'),
            current.month,
            current.strftime('%B'),
            (current.month-1)//3 + 1,                # quarter number
            f"Q{(current.month-1)//3 + 1}",
            1 if current.month <= 6 else 2,         # half year
            "H1" if current.month <= 6 else "H2",
            current.year,
            current.weekday() >= 5,                 # weekend
            False                                   # holiday placeholder
        ))
        current += delta

    insert_sql = """
    INSERT INTO shopzada.dimDate(
        date_key, date_full, date_day, date_day_name, date_month, date_month_name,
        date_quarter, date_quarter_name, date_half_year, date_half_year_name,
        date_year, date_is_weekend, date_is_holiday
    )
    VALUES %s
    ON CONFLICT (date_key) DO NOTHING;
    """
    execute_values(cursor, insert_sql, rows)
    conn.commit()

    # Confirm load
    cursor.execute("SELECT COUNT(*) FROM shopzada.dimDate;")
    total_rows = cursor.fetchone()[0]
    print(f"dimDate loaded with {total_rows} rows")

    cursor.close()
    conn.close()

with DAG(
    'load_dim_date',
    default_args=default_args,
    description='Create and load dimDate from 1950 to 2030',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dimension', 'date'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_dim_date_table',
        python_callable=create_dim_date_table
    )

    task_load_dates = PythonOperator(
        task_id='load_dim_date_task',
        python_callable=load_dim_date
    )

    task_create_table >> task_load_dates
