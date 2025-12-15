# ============================================================
# Cleaning Script — Operations / Orders
# Purpose: Prepare order-level transactional data
# Layer: Cleaning (NO joins, NO surrogate keys)
# ============================================================

import pandas as pd
import re
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

RAW_DIR = Path("/data_files/Operations Department")
OUT_DIR = Path("/clean_data/operations")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ORDER_FILES = sorted(RAW_DIR.glob("order_data_*.csv"))

# ============================================================
# HELPERS
# ============================================================

def parse_days(val):
    """
    Extract integer days from strings like:
    '13days', '8days', '15 days'
    """
    if pd.isna(val):
        return pd.NA
    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else pd.NA


def to_date_key(dt: pd.Series) -> pd.Series:
    """
    Convert datetime to YYYYMMDD integer.
    """
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def load_and_concat(files):
    if not files:
        raise FileNotFoundError(
            "❌ No order_data_*.csv files found in Operations Department"
        )

    frames = []
    for f in files:
        df = pd.read_csv(f)
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


# ============================================================
# CLEANING LOGIC
# ============================================================

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:

    # Normalize IDs
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["user_id"] = df["user_id"].astype(str).str.strip()

    # Parse transaction_date
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce"
    )

    # Add date_key (allowed in cleaning)
    df["transaction_date_key"] = to_date_key(df["transaction_date"])

    # Parse estimated arrival
    df["estimated_arrival_days"] = (
        df["estimated arrival"].apply(parse_days)
    )

    # Drop raw text column
    df = df.drop(columns=["estimated arrival"])

    # Sort for downstream joins
    df = df.sort_values(["transaction_date", "order_id"])

    return df


def split_clean_and_issues(df: pd.DataFrame):
    """
    Orders are atomic:
    - order_id must exist
    - user_id must exist
    - transaction_date must be valid
    """
    required = [
        "order_id",
        "user_id",
        "transaction_date",
        "transaction_date_key",
        "estimated_arrival_days",
    ]

    issue_mask = df[required].isna().any(axis=1)

    clean_df = df.loc[~issue_mask].copy()
    issues_df = df.loc[issue_mask].copy()

    return clean_df, issues_df


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    clean_df.to_parquet(OUT_DIR / f"{name}.parquet", index=False)

    issues_df.fillna("").to_csv(
        OUT_DIR / f"{name}_issues.csv", index=False
    )

    print(
        f"[OK] {name}: clean={len(clean_df):,}, issues={len(issues_df):,}"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    print("\n=== Cleaning Operations Orders ===\n")

    orders_raw = load_and_concat(ORDER_FILES)
    orders_clean = clean_orders(orders_raw)
    clean_df, issues_df = split_clean_and_issues(orders_clean)

    save_outputs(clean_df, issues_df, "orders_clean")

    print("\nOrders cleaning completed ✓\n")


if __name__ == "__main__":
    main()
