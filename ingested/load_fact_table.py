import pandas as pd
import psycopg2
from psycopg2 import sql
import glob

# -----------------------------
# PostgreSQL connection
# -----------------------------
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "kestra"
DB_USER = "kestra"
DB_PASS = "k3str4"

SCHEMA_NAME = "shopzada"
TABLE_NAME = "factlineitem"

# -----------------------------
# CSV locations inside container
# -----------------------------
CSV_ORDER_DATA_FOLDER = "/ingested/operations/cleaned/order_data*.csv"
CSV_LINE_ITEM_FOLDER = "/ingested/operations/cleaned/line_item_data_prices*.csv"
CSV_ORDER_MERCHANT = "/ingested/enterprise/order_with_merchant_data_all_clean.csv"
CSV_CAMPAIGN = "/ingested/marketing/transactional_campaign_data.csv"
CSV_DELAYS = "/ingested/operations/cleaned/order_delays.csv"


def load_factlineitem():
    conn = None
    cursor = None
    try:
        # -----------------------------
        # Helper to load CSVs with pattern
        # -----------------------------
        def load_csv_pattern(pattern):
            files = glob.glob(pattern)
            df_list = []
            for f in files:
                df_tmp = pd.read_csv(f)
                df_tmp = df_tmp.drop(columns=[c for c in df_tmp.columns if "Unnamed" in c], errors='ignore')
                df_list.append(df_tmp)
            return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

        # Load CSVs
        df_orders = load_csv_pattern(CSV_ORDER_DATA_FOLDER)
        df_line = load_csv_pattern(CSV_LINE_ITEM_FOLDER)
        df_order_merchant = pd.read_csv(CSV_ORDER_MERCHANT).drop(
            columns=[c for c in pd.read_csv(CSV_ORDER_MERCHANT).columns if "Unnamed" in c], errors='ignore'
        )
        df_campaign = pd.read_csv(CSV_CAMPAIGN).drop(
            columns=[c for c in pd.read_csv(CSV_CAMPAIGN).columns if "Unnamed" in c], errors='ignore'
        )
        df_delays = pd.read_csv(CSV_DELAYS).drop(
            columns=[c for c in pd.read_csv(CSV_DELAYS).columns if "Unnamed" in c], errors='ignore'
        )

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
        # Normalize columns
        # -----------------------------
        df = df.rename(columns={
            "price": "price_per_quantity",
            "availed": "campaign_availed",
            "delay in days": "delay_in_days"
        })

        # Boolean column
        df["campaign_availed"] = df.get("campaign_availed", False).fillna(False).astype(bool)

        # Transaction date → transaction_date_key
        if "transaction_date" in df.columns:
            df["transaction_date_key"] = pd.to_datetime(df["transaction_date"], errors="coerce")
            df = df.dropna(subset=["transaction_date_key"])
            df["transaction_date_key"] = df["transaction_date_key"].dt.strftime("%Y%m%d").astype(int)
            df = df.drop(columns=["transaction_date"])

        # Fill numeric columns
        for col in ["quantity", "estimated_arrival_in_days", "delay_in_days", "price_per_quantity"]:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        df["quantity"] = df["quantity"].astype(int)
        df["estimated_arrival_in_days"] = df["estimated_arrival_in_days"].astype(int)
        df["delay_in_days"] = df["delay_in_days"].astype(int)

        print(df.describe())

        # -----------------------------
        # Connect to Postgres
        # -----------------------------
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        print("Connected to Postgres!")

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

        # -----------------------------
        # Map business IDs → surrogate keys
        # -----------------------------
        def map_ids(df, col_name, mapping):
            if col_name in df.columns:
                df[col_name.replace("_id", "_key")] = df[col_name].map(mapping)
                # Convert pd.NA → None for psycopg2
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
        # Prepare data for insert
        # -----------------------------
        columns = list(df.columns)
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(SCHEMA_NAME),
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        # Convert pd.NA → None for all columns
        data = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
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
    load_factlineitem()