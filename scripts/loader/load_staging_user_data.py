import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

def load_staging_user_data():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    df = pd.read_csv("/data_files/user_data.csv")

    records = df.values.tolist()

    insert_sql = """
        INSERT INTO staging_user_data (
            user_id, creation_date, name, street, state, city, country,
            birthdate, gender, device_address, user_type,
            creation_date_key, birth_date_key, user_id_orig, user_key
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, records)
    conn.commit()
    cur.close()
