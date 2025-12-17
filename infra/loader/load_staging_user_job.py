import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_staging_user_job():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    df = pd.read_csv("/data_files/user_job.csv")
    records = df.values.tolist()

    insert_sql = """
        INSERT INTO staging_user_job (
            user_key, name, job_title, job_level
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, records)
    conn.commit()
    cur.close()
