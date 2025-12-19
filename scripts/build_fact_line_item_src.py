

# FACT SOURCE BUILDER — fact_line_item_src
# Grain: order × product (row-aligned)

import pandas as pd
import re
from pathlib import Path

CLEAN_DIR = Path("/clean_data")
OPS_DIR = CLEAN_DIR / "operations"
ENT_DIR = CLEAN_DIR / "enterprise"
MKT_DIR = CLEAN_DIR / "marketing"

OUT_DIR = CLEAN_DIR / "facts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRODUCT_FILE  = OPS_DIR / "line_item_products_clean.csv"
PRICE_FILE    = OPS_DIR / "line_item_prices_clean.csv"
ORDER_FILE    = OPS_DIR / "orders_clean.csv"
MERCH_FILE    = ENT_DIR / "order_with_merchant_clean.csv"
CAMPAIGN_FILE = MKT_DIR / "transactional_campaign_clean.csv"

def parse_quantity(val):
    if pd.isna(val):
        return None
    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else None

def make_date_key(series):
    return (
        pd.to_datetime(series, errors="coerce")
        .dt.strftime("%Y%m%d")
        .astype("Int64")
    )

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("\n=== Building fact_line_item_src (row-aligned) ===\n")

    # ---------- Load products ----------
    products = pd.read_csv(PRODUCT_FILE)
    products = products.drop(columns=["Unnamed: 0"], errors="ignore")
    products["line_number"] = products.groupby("order_id").cumcount()

    # ---------- Load prices ----------
    prices = pd.read_csv(PRICE_FILE)
    prices = prices.drop(columns=["Unnamed: 0"], errors="ignore")
    prices["quantity"] = prices["quantity"].apply(parse_quantity)
    prices = prices.rename(columns={"price": "unit_price"})
    prices["line_number"] = prices.groupby("order_id").cumcount()

    # ---------- Join products + prices (SAFE) ----------
    line_items = products.merge(
        prices,
        on=["order_id", "line_number"],
        how="inner"
    )

    # ---------- Orders ----------
    orders = pd.read_csv(ORDER_FILE)
    orders["date_key"] = make_date_key(orders["transaction_date"])
    orders = orders[["order_id", "user_id", "date_key"]]

    # ---------- Merchant / Staff ----------
    merch = pd.read_csv(MERCH_FILE)
    merch = merch[["order_id", "merchant_id", "staff_id"]]

    # ---------- Campaign (optional) ----------
    if CAMPAIGN_FILE.exists():
        camp = pd.read_csv(CAMPAIGN_FILE)
        camp = camp[camp["availed"] == 1]
        camp = camp[["order_id", "campaign_id"]]
    else:
        camp = pd.DataFrame(columns=["order_id", "campaign_id"])

    # ---------- Build fact source ----------
    fact = (
        line_items
        .merge(orders, on="order_id", how="left")
        .merge(merch, on="order_id", how="left")
        .merge(camp, on="order_id", how="left")
    )

    # ---------- Measures ----------
    fact["line_amount"] = fact["unit_price"] * fact["quantity"]

    # ---------- Column contract ----------
    fact = fact[[
        "order_id",
        "line_number",
        "product_id",
        "product_name",
        "user_id",
        "merchant_id",
        "staff_id",
        "unit_price",
        "quantity",
        "line_amount",
        "date_key",
        "campaign_id",
    ]]

    # ---------- Validate ----------
    required = [
        "order_id", "product_id", "user_id"
    ]

    issues = fact[fact[required].isna().any(axis=1)]
    clean  = fact.dropna(subset=required)

    # ---------- Save ----------
    clean.to_csv(OUT_DIR / "fact_line_item_src.csv", index=False)
    clean.to_parquet(OUT_DIR / "fact_line_item_src.parquet", index=False)
    issues.to_csv(OUT_DIR / "fact_line_item_src_issues.csv", index=False)

    print(f"[OK] Rows: {len(clean):,}")
    print(f"[WARN] Issues: {len(issues):,}")
    print("\n=== Done ===\n")

if __name__ == "__main__":
    main()
