import argparse
from time import time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url          # path to CSV

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    t_start_total = time()

    # Save CSV as Parquet
    parquet_path = str(Path(url).with_suffix('.parquet'))
    df_full = pd.read_csv(url)
    df_full.to_parquet(parquet_path, index=False)
    print(f"Saved Parquet file to {parquet_path}")

    # Chunked ingestion (unchanged)
    df_iter = pd.read_csv(url, chunksize=100000)

    first_chunk = next(df_iter)
    first_chunk.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )
    first_chunk.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
    )

    for chunk in df_iter:
        t_start = time()
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )
        t_end = time()
        print(f"Inserted another chunk..., took {t_end - t_start:.3f} seconds")

    t_end_total = time()
    print(f"Finished ingest for {table_name}, total {t_end_total - t_start_total:.3f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest order_data CSV to Postgres")

    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--url", required=True)   # CSV path

    args = parser.parse_args()
    main(args)
