"""
import pandas as pd
import glob
# -----------------------------
# Parquet file locations (same as your DAG)
# -----------------------------
ORDER_DATA_FOLDER = "ingested/operations/parq files/order_data*.parquet"
LINE_ITEM_FOLDER = "ingested/operations/parq files/line_item_data_prices*.parquet"
ORDER_MERCHANT = "ingested/enterprise/order_with_merchant_data_all.parquet"
CAMPAIGN = "ingested/marketing/transactional_campaign_data.parquet"
DELAYS = "ingested/operations/parq files/order_delays.parquet"

# -----------------------------
# Helper to read parquet patterns
# -----------------------------
def load_parquet_pattern(pattern):
    files = glob.glob(pattern)
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# -----------------------------
# Read all parquet files
# -----------------------------
df_orders = load_parquet_pattern(ORDER_DATA_FOLDER)
df_line = load_parquet_pattern(LINE_ITEM_FOLDER)
df_order_merchant = pd.read_parquet(ORDER_MERCHANT)
df_campaign = pd.read_parquet(CAMPAIGN)
df_delays = pd.read_parquet(DELAYS)

# -----------------------------
# Keep only relevant columns
# -----------------------------
df_orders_small = df_orders[['order_id', 'user_id', 'transaction_date', 'estimated_arrival_in_days']]
df_campaign_small = df_campaign[['order_id', 'campaign_id', 'availed']]

# -----------------------------
# Merge all data
# -----------------------------
df = df_line.merge(df_orders_small, on="order_id", how="left") \
            .merge(df_order_merchant, on="order_id", how="left") \
            .merge(df_campaign_small, on="order_id", how="left") \
            .merge(df_delays, on="order_id", how="left")

# -----------------------------
# Remove unwanted columns from merges
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
# Preview results
# -----------------------------
print("Columns:", df.columns.tolist())
print("\nFirst 10 rows:")
print(df.head(10))

print("\nSummary statistics:")
print(df.describe(include='all'))

# Optional: save to CSV for quick inspection
df.to_csv("factlineitem_preview.csv", index=False)
print("Saved preview CSV as factlineitem_preview.csv in the project folder")

=====================================

import pandas as pd

# Load the preview CSV
df = pd.read_csv("factlineitem_preview.csv")

print(df.isna().sum())

"""

