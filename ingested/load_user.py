import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL connection parameters inside Docker
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

# CSVs inside container
CSV_CREDIT = "/ingested/customer/user_credit_card.csv"
CSV_DATA = "/ingested/customer/user_data.csv"
CSV_JOB  = "/ingested/customer/user_job_clean.csv"

SCHEMA_NAME = "shopzada"
TABLE_NAME = "dimuser"

def load_dimuser():
    conn = None
    cursor = None
    try:
        # Load CSVs
        df_credit = pd.read_csv(CSV_CREDIT)
        df_data   = pd.read_csv(CSV_DATA)
        df_job    = pd.read_csv(CSV_JOB)

        # Merge on user_id
        df = df_data.merge(df_credit[['user_id', 'credit_card_number', 'issuing_bank']], on='user_id', how='left')
        df = df.merge(df_job[['user_id', 'job_title', 'job_level']], on='user_id', how='left')

        print(f"Combined CSV shape: {df.shape}")
        print(df.head())

        # Rename columns to match table
        df = df.rename(columns={
            "name": "user_name",
            "street": "user_street",
            "state": "user_state",
            "city": "user_city",
            "country": "user_country",
            "gender": "user_gender",
            "device_address" : "user_device_address",
            "user_type": "user_user_type",
            "credit_card_number": "user_credit_card",
            "issuing_bank": "user_issuing_bank",
            "job_title": "user_job_title",
            "job_level": "user_job_level",
            "creation_date": "user_creation_date",
            "birthdate": "user_birth_date"
        })

        # Map dates to dimDate keys
        for col, key_name in [("user_creation_date", "user_creation_date_key"),
                              ("user_birth_date", "user_birth_date_key")]:
            df[key_name] = pd.to_datetime(df[col]).dt.strftime("%Y%m%d").astype(int)
            df = df.drop(columns=[col])

        print("Columns after rename and date mapping:")
        print(df.columns)

        # Connect to Postgres
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        print("Connected to Postgres!")

        # Build INSERT
        columns = list(df.columns)
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT (user_id) DO NOTHING").format(
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        data = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(query.as_string(conn), data)
        conn.commit()

        print(f"{len(data)} rows inserted into {SCHEMA_NAME}.{TABLE_NAME}!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_dimuser()
