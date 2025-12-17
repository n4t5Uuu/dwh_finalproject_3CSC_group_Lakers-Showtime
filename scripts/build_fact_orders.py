# ============================================================
#              FACT BUILDER FOR ORDERS + LINE ITEMS
# ============================================================

import pandas as pd
from pathlib import Path

# ---------------- PATH SETUP ---------------- #

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[0]

CLEAN_DIR = Path("/clean_data")
OPS_DIR = CLEAN_DIR / "operations"
MKT_DIR = CLEAN_DIR / "marketing"

ORDERS_FILE = OPS_DIR / "orders_final_ready.csv"
LINEITEM_FILE = OPS_DIR / "line_item_final.csv"
CAMPAIGN_FILE = MKT_DIR / "campaign_data.csv"
TXN_CAMPAIGN_FILE = MKT_DIR / "transactional_campaign_data.csv"

OUT_DIR = CLEAN_DIR / "facts"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
#                   UTILITIES
# ============================================================


def ensure_transaction_date(df):
    """Look for any date column and rename it to transaction_date."""
    possible = ["transaction_date", "transactionDate", "date",
                "order_date", "purchase_date"]
    for col in possible:
        if col in df.columns:
            df = df.rename(columns={col: "transaction_date"})
            return df
    raise ValueError(" No usable transaction date column found in orders_final_ready.csv")


def make_date_key(df, colname):
    """Convert a date column into a Kimball-style date_key: YYYYMMDD."""
    df[colname] = pd.to_datetime(df[colname], errors="coerce")
    df["date_key"] = df[colname].dt.strftime("%Y%m%d").astype("Int64")
    return df


def split_clean_issues(df, required_cols):
    """Rows missing any required FK/measure become issues."""
    bad_mask = df[required_cols].isna().any(axis=1)
    issues = df[bad_mask].copy()
    clean = df[~bad_mask].copy()
    return clean, issues


def save(df, name):
    df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    df.to_parquet(OUT_DIR / f"{name}.parquet", index=False)
    print(f"[OK] Saved {name}")


# ============================================================
#                   MAIN FACT BUILDER
# ============================================================

def main():

    print("\n========== BUILDING FACT TABLES ==========\n")

    # ---------------- Load Orders ---------------- #
    if not ORDERS_FILE.exists():
        raise FileNotFoundError(f"Missing orders file: {ORDERS_FILE}")

    orders = pd.read_csv(ORDERS_FILE)
    orders = ensure_transaction_date(orders)
    orders = make_date_key(orders, "transaction_date")

    print(f"Loaded orders: {len(orders):,}")

    # ---------------- Load Line Items ---------------- #
    if not LINEITEM_FILE.exists():
        raise FileNotFoundError(f"Missing lineitem file: {LINEITEM_FILE}")

    lineitems = pd.read_csv(LINEITEM_FILE)
    print(f"Loaded line items: {len(lineitems):,}")

    # ---------------- Merge Orders + Line Items ---------------- #
    fact = lineitems.merge(
        orders,
        on="order_id",
        how="left",
        suffixes=("_li", "_ord")
    )

    # Rename to match star schema
    fact = fact.rename(columns={
        "price": "price_per_quantity",
        "quantity_clean": "quantity",
    })

    # Required dimensions for a factLineItem row
    required = [
        "user_key",
        "product_id",
        "merchant_key",
        "staff_key",
        "date_key",
        "price_per_quantity",
        "quantity"
    ]

    clean_fact, issues_fact = split_clean_issues(fact, required)

    save(clean_fact, "factLineItem")
    save(issues_fact, "factLineItem_issues")

    print(f"[OK] factLineItem rows: {len(clean_fact):,}")
    print(f"[OK] factLineItem_issues rows: {len(issues_fact):,}\n")

    # ============================================================
    #            BUILD factCampaignAvailed
    # ============================================================

    if not TXN_CAMPAIGN_FILE.exists():
        print("[WARN] No marketing transactional campaign data found.")
        return

    txn = pd.read_csv(TXN_CAMPAIGN_FILE)
    print(f"Loaded transactional campaigns: {len(txn):,}")

    # Add date_key for campaign events
    txn = make_date_key(txn, "transaction_date")

    # Required dimensions for factCampaignAvailed
    required_campaign = ["order_id", "campaign_id", "date_key"]

    clean_txn, issues_txn = split_clean_issues(txn, required_campaign)

    save(clean_txn, "factCampaignAvailed")
    save(issues_txn, "factCampaignAvailed_issues")

    print(f"[OK] factCampaignAvailed rows: {len(clean_txn):,}")
    print(f"[OK] factCampaignAvailed_issues rows: {len(issues_txn):,}\n")

    print("\n========== FACT BUILD COMPLETE ==========\n")


if __name__ == "__main__":
    main()
