import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_staging_user_credit_card():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    df = pd.read_csv("/data_files/user_credit_card.csv")
    records = df.values.tolist()

    insert_sql = """
        INSERT INTO staging_user_credit_card (
            user_key, name, credit_card_number, issuing_bank
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, records)
    conn.commit()
    cur.close()
