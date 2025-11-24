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
CSV_FILE_PATH = "/ingested/business/product_list.csv"

# Schema + Table name
SCHEMA_NAME = "shopzada"
TABLE_NAME = "dimproduct"

def load_dimproduct():
    conn = None
    cursor = None
    try:
        # Load CSV
        df = pd.read_csv(CSV_FILE_PATH)
        print(df.head())
        print(f"\nCSV loaded: {df.shape[0]} rows")

        # Rename columns to match table
        df = df.rename(columns={
            "product_id": "product_id",
            "product_name": "product_name",
            "product_type": "product_type",
            "price": "product_price"
        })

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

        # Build INSERT statement
        columns = list(df.columns)
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT (product_id) DO NOTHING").format(
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
    load_dimproduct()
