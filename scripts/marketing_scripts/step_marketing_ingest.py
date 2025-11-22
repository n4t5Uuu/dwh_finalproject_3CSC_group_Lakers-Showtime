# step_marketing_ingest.py

import re
from pathlib import Path

import pandas as pd

# ================== CONFIG ================== #
RAW_DIR = Path(".")  # folder where your CSVs are located

CAMPAIGN_FILE = RAW_DIR / "campaign_clean.csv"
TXN_CAMPAIGN_FILE = RAW_DIR / "transactional_campaign_data.csv"

OUT_DIR = Path("ingested") / "marketing"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


def parse_discount_value(s: str):
    """Extract numeric discount from messy strings like '1%', '1pct', '10%%', '1percent'."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    m = re.search(r"(\d+(\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(1))


def load_campaign_data(path: Path) -> pd.DataFrame:
    """
    campaign_clean.csv is in a single-column, tab-separated format.
    We'll:
    - read as-is
    - split that single column on '\t'
    - build proper columns
    - parse discount to numeric percent
    """
    raw = pd.read_csv(path)

    # There should be exactly one column with a header that contains tab-separated names
    single_col_name = raw.columns[0]

    # Split each row on '\t' into 5 parts: [index, campaign_id, name, desc, discount]
    parts = raw[single_col_name].astype(str).str.split("\t", n=4, expand=True)

    # Build clean dataframe
    df = pd.DataFrame(
        {
            "campaign_id": parts[1].astype(str),
            "campaign_name": parts[2].astype(str),
            "campaign_description": parts[3].astype(str),
            "discount_raw": parts[4].astype(str),
        }
    )

    # Numeric discount as percent (1, 5, 10, 20, etc.)
    df["discount_percent"] = df["discount_raw"].apply(parse_discount_value)

    return df


def parse_estimated_arrival_days(s: str):
    """Convert '10days' -> 10, '3days' -> 3, etc."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    return int(m.group(1))


def load_transactional_campaign_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean transactional_campaign_data.csv"""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Enforce basic types
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["order_id"] = df["order_id"].astype(str)

    # Parse date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Keep raw string + numeric days
    df["estimated_arrival_raw"] = df["estimated arrival"].astype(str)
    df["estimated_arrival_days"] = df["estimated_arrival_raw"].apply(
        parse_estimated_arrival_days
    )

    # Make column names warehouse-friendly
    df = df.drop(columns=["estimated arrival"])
    # availed already int 0/1, keep as-is

    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Split into clean rows vs issue rows.

    - Issues = duplicates (by key_cols) OR any null/NaT
    - Formatting issues (discount strings, quotes in description) are NOT treated as issues here.
      Those will be cleaned by separate scripts (e.g., campaign_clean.py).
    """
    if key_cols:
        duplicate_mask = df.duplicated(subset=key_cols, keep=False)
    else:
        duplicate_mask = df.duplicated(keep=False)

    null_mask = df.isna().any(axis=1)

    bad_mask = duplicate_mask | null_mask

    issues_df = df.loc[bad_mask].copy()
    clean_df = df.loc[~bad_mask].copy()

    return clean_df, issues_df


def save_outputs(clean_df: pd.DataFrame, issues_df: pd.DataFrame, name: str):
    """
    Save:
      - clean CSV + Parquet as {name}.csv / {name}.parquet
      - issues CSV as {name}_issues.csv
    """
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    # Clean data
    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    # Issues: replace NaN/NaT with empty string for easier manual review
    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows:  {len(clean_df):,}")
    print(f"  Issue rows:  {len(issues_df):,}")
    print(f"  CSV:         {csv_path}")
    print(f"  Parquet:     {parquet_path}")
    print(f"  Issues CSV:  {issues_path}\n")


def main():
    print("=== Ingesting Marketing department datasets ===\n")

    # ---------- campaign_data ---------- #
    print("Loading campaign_clean.csv ...")
    campaigns_raw = load_campaign_data(CAMPAIGN_FILE)
    print(campaigns_raw.head())
    print(campaigns_raw.dtypes, "\n")

    # Issues: duplicate campaign_id or any null/NaT
    campaigns_clean, campaigns_issues = split_clean_and_issues(
        campaigns_raw, key_cols=["campaign_id"]
    )
    save_outputs(campaigns_clean, campaigns_issues, "campaign_data")

    # ---------- transactional_campaign_data ---------- #
    print("Loading transactional_campaign_data.csv ...")
    txn_campaigns_raw = load_transactional_campaign_data(TXN_CAMPAIGN_FILE)
    print(txn_campaigns_raw.head())
    print(txn_campaigns_raw.dtypes, "\n")

    # Issues: duplicate (order_id, campaign_id) or any null/NaT
    txn_campaigns_clean, txn_campaigns_issues = split_clean_and_issues(
        txn_campaigns_raw, key_cols=["order_id", "campaign_id"]
    )
    save_outputs(
        txn_campaigns_clean,
        txn_campaigns_issues,
        "transactional_campaign_data",
    )

    print("Marketing ingestion complete ✅")


if __name__ == "__main__":
    main()
