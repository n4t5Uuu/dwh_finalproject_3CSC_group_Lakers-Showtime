import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL connection parameters inside Docker
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

# CSV inside container
CSV_FILE_PATH = "/ingested/enterprise/staff_data_clean.csv"

# Schema + Table name
SCHEMA_NAME = "shopzada"
TABLE_NAME = "dimstaff"

def load_dimstaff():
    try:
        #Load CSV
        df = pd.read_csv(CSV_FILE_PATH)
        print(df.head())

        print(f"\nCSV loaded: {df.shape[0]} rows")

        df = df.rename(columns={
            "name":"staff_name",
            "job_level": "staff_job_level",
            "street": "staff_street",
            "state": "staff_state",
            "city": "staff_city",
            "country": "staff_country",
            "contact_number": "staff_contact_number"
        })
        
        # Map date to dimDate
        # Convert YYYY-MM-DD → int YYYYMMDD
        df["staff_creation_date_key"] = (
            pd.to_datetime(df["creation_date"])
            .dt.strftime("%Y%m%d")
            .astype(int)
        )

        # Drop original creation_date
        df = df.drop(columns=["creation_date"])

        print("Columns after rename:")
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

        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        # Convert DataFrame → list of tuples
        data = [tuple(row) for row in df.to_numpy()]

        # Execute batch insert
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
    load_dimstaff()