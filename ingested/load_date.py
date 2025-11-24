import pandas as pd
import psycopg2
from psycopg2 import sql

# PostgreSQL connection parameters
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

# Define date range
START_DATE = "1950-01-01"
END_DATE = "2030-12-31"

def load_dim_date():
    try:
        # Generate date range
        dates = pd.date_range(start=START_DATE, end=END_DATE)
        df = pd.DataFrame(dates, columns=["date_full"])

        # Extract date components
        df["date_key"] = df["date_full"].dt.strftime("%Y%m%d").astype(int)
        df["date_day"] = df["date_full"].dt.day
        df["date_day_name"] = df["date_full"].dt.day_name()
        df["date_month"] = df["date_full"].dt.month
        df["date_month_name"] = df["date_full"].dt.month_name()
        df["date_quarter"] = df["date_full"].dt.quarter
        df["date_quarter_name"] = "Q" + df["date_quarter"].astype(str)
        df["date_half_year"] = df["date_full"].dt.month.apply(lambda x: 1 if x <= 6 else 2)
        df["date_half_year_name"] = df["date_half_year"].apply(lambda x: f"H{x}")
        df["date_year"] = df["date_full"].dt.year
        df["date_is_weekend"] = df["date_full"].dt.weekday >= 5
        df["date_is_holiday"] = False  # default, can be updated later

        # Connect to Postgres
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        # Build insert query
        columns = list(df.columns)
        query = sql.SQL("INSERT INTO shopzada.dimDate ({}) VALUES ({}) ON CONFLICT (date_key) DO NOTHING").format(
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        # Convert dataframe to list of tuples
        data = [tuple(x) for x in df.to_numpy()]

        # Execute batch insert
        cursor.executemany(query.as_string(conn), data)
        conn.commit()

        print(f"{len(data)} rows inserted into shopzada.dimDate successfully!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_dim_date()