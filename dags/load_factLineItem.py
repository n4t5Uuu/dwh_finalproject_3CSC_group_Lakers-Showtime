from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2 import sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

SCHEMA_NAME = "shopzada"
TABLE_NAME = "factlineitem"

# -----------------------------
# Parquet file locations
# -----------------------------
ORDER_DATA_FOLDER = "/clean_data/operations/parq files/order_data*.parquet"
LINE_ITEM_FOLDER = "/clean_data/operations/parq files/line_item_data_prices*.parquet"
ORDER_MERCHANT = "/clean_data/enterprise/order_with_merchant_data_all.parquet"
CAMPAIGN = "/clean_data/marketing/transactional_campaign_data.parquet"
DELAYS = "/clean_data/operations/parq files/order_delays.parquet"


def create_factlineitem_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        transaction_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_key                   BIGINT,
        product_key                BIGINT,
        campaign_key               BIGINT,
        merchant_key               BIGINT,
        staff_key                  BIGINT,
        transaction_date_key       BIGINT,
        price_per_quantity         NUMERIC(20,2) CHECK (price_per_quantity >= 0),
        quantity                   BIGINT CHECK (quantity >= 0),
        estimated_arrival_in_days  BIGINT CHECK (estimated_arrival_in_days >= 0),
        delay_in_days              BIGINT CHECK (delay_in_days >= 0),
        campaign_availed           BOOLEAN DEFAULT FALSE,
        CONSTRAINT fk_fact_user       FOREIGN KEY (user_key) REFERENCES {SCHEMA_NAME}.dimUser(user_key)
            ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_fact_product    FOREIGN KEY (product_key) REFERENCES {SCHEMA_NAME}.dimProduct(product_key)
            ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_fact_campaign   FOREIGN KEY (campaign_key) REFERENCES {SCHEMA_NAME}.dimCampaign(campaign_key)
            ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_fact_merchant   FOREIGN KEY (merchant_key) REFERENCES {SCHEMA_NAME}.dimMerchant(merchant_key)
            ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_fact_staff      FOREIGN KEY (staff_key) REFERENCES {SCHEMA_NAME}.dimStaff(staff_key)
            ON UPDATE CASCADE ON DELETE SET NULL,
        CONSTRAINT fk_fact_txn_date   FOREIGN KEY (transaction_date_key) REFERENCES {SCHEMA_NAME}.dimDate(date_key)
            ON UPDATE CASCADE ON DELETE SET NULL
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{SCHEMA_NAME}.{TABLE_NAME} table ensured.")


def load_factlineitem_data():
    import glob
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # -----------------------------
    # Helper to read parquet pattern
    # -----------------------------
    def load_parquet_pattern(pattern):
        files = glob.glob(pattern)
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    df_orders = load_parquet_pattern(ORDER_DATA_FOLDER)
    df_line = load_parquet_pattern(LINE_ITEM_FOLDER)
    df_order_merchant = pd.read_parquet(ORDER_MERCHANT)
    df_campaign = pd.read_parquet(CAMPAIGN)
    df_delays = pd.read_parquet(DELAYS)

    # -----------------------------
    # Keep only necessary columns
    # -----------------------------
    df_orders_small = df_orders[['order_id', 'user_id', 'transaction_date', 'estimated_arrival_in_days']]
    df_campaign_small = df_campaign[['order_id', 'campaign_id', 'availed']]

    # Merge all data
    df = df_line.merge(df_orders_small, on="order_id", how="left") \
                .merge(df_order_merchant, on="order_id", how="left") \
                .merge(df_campaign_small, on="order_id", how="left") \
                .merge(df_delays, on="order_id", how="left")

    # -----------------------------
    # Remove any unwanted merge/index columns
    # -----------------------------
    df = df.loc[:, ~df.columns.str.contains(r'^Unnamed|_x$|_y$')]

    # -----------------------------
    # Normalize columns
    # -----------------------------
    df = df.rename(columns={
        "price": "price_per_quantity",
        "availed": "campaign_availed",
        "delay in days": "delay_in_days"
    })
    df["campaign_availed"] = df.get("campaign_availed", False).fillna(False).astype(bool)

    if "transaction_date" in df.columns:
        df["transaction_date_key"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        df = df.dropna(subset=["transaction_date_key"])
        df["transaction_date_key"] = df["transaction_date_key"].dt.strftime("%Y%m%d").astype(int)
        df = df.drop(columns=["transaction_date"])

    for col in ["quantity", "estimated_arrival_in_days", "delay_in_days", "price_per_quantity"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    df["quantity"] = df["quantity"].astype(int)
    df["estimated_arrival_in_days"] = df["estimated_arrival_in_days"].astype(int)
    df["delay_in_days"] = df["delay_in_days"].astype(int)

    # -----------------------------
    # Get dimension mappings
    # -----------------------------
    def get_mapping(table, id_col, key_col):
        cursor.execute(sql.SQL("SELECT {}, {} FROM {}.{}").format(
            sql.Identifier(id_col),
            sql.Identifier(key_col),
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(table)
        ))
        return dict(cursor.fetchall())

    user_map = get_mapping("dimuser", "user_id", "user_key")
    product_map = get_mapping("dimproduct", "product_id", "product_key")
    merchant_map = get_mapping("dimmerchant", "merchant_id", "merchant_key")
    staff_map = get_mapping("dimstaff", "staff_id", "staff_key")
    campaign_map = get_mapping("dimcampaign", "campaign_id", "campaign_key")

    def map_ids(df, col_name, mapping):
        if col_name in df.columns:
            df[col_name.replace("_id", "_key")] = df[col_name].map(mapping)
            df[col_name.replace("_id", "_key")] = df[col_name.replace("_id", "_key")].where(
                pd.notna(df[col_name.replace("_id", "_key")]), None
            )

    map_ids(df, "user_id", user_map)
    map_ids(df, "product_id", product_map)
    map_ids(df, "merchant_id", merchant_map)
    map_ids(df, "staff_id", staff_map)
    map_ids(df, "campaign_id", campaign_map)

    # Drop original ID columns
    df = df.drop(columns=["user_id","product_id","merchant_id","staff_id","campaign_id","order_id"], errors='ignore')

    # -----------------------------
    # Keep only columns that exist in table
    # -----------------------------
    allowed_columns = [
        "user_key", "product_key", "campaign_key", "merchant_key", "staff_key",
        "transaction_date_key", "price_per_quantity", "quantity",
        "estimated_arrival_in_days", "delay_in_days", "campaign_availed"
    ]
    df = df[[col for col in allowed_columns if col in df.columns]]

    # -----------------------------
    # Insert into fact table
    # -----------------------------
    columns = list(df.columns)
    insert_sql = sql.SQL("""
        INSERT INTO {}.{} ({})
        VALUES ({})
        ON CONFLICT DO NOTHING
    """).format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    data = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
    cursor.executemany(insert_sql.as_string(conn), data)
    conn.commit()
    print(f"{len(data)} rows inserted into {SCHEMA_NAME}.{TABLE_NAME}!")

    cursor.close()
    conn.close()



with DAG(
    'load_factlineitem',
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=['fact', 'lineitem']
) as dag:

    task_create_table = PythonOperator(
        task_id='create_factlineitem_table',
        python_callable=create_factlineitem_table
    )

    task_load_data = PythonOperator(
        task_id='load_factlineitem_data',
        python_callable=load_factlineitem_data
    )

    task_create_table >> task_load_data