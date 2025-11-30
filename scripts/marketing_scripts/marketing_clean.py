# Cleaning Script for Marketing Department Tables

import re
from pathlib import Path

import pandas as pd

# ================== CONFIG ================== #
# Get Root:
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
RAW_DIR = PROJECT_ROOT / "data_files" / "Marketing Department"

# Now we read the RAW campaign_data.csv directly
CAMPAIGN_FILE = RAW_DIR / "campaign_data.csv"
TXN_CAMPAIGN_FILE = RAW_DIR / "transactional_campaign_data.csv"

OUT_DIR = Path("clean_data") / "marketing"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


# ---------- Cleaning helpers (from former campaign_clean.py) ---------- #

def normalize_discount(val):
    """
    Make discount values consistent and numeric.

    Examples:
      '1%'        -> 1
      '1pct'      -> 1
      '5prcnt'    -> 5
      '10PERCENT' -> 10
      '10%%'      -> 10
    """
    if pd.isna(val):
        return pd.NA

    text = str(val).strip().lower()

    # Find the first number (int or float)
    m = re.search(r"(\d+(\.\d+)?)", text)
    if not m:
        return pd.NA

    num = float(m.group(1))
    return int(num) if num.is_integer() else num


def clean_description(desc: str) -> str:
    """
    Remove ALL quotation marks and extra spaces.

    Example:
      '"Twee retro ... health." - Raleigh Senger"' ->
      'Twee retro ... health. - Raleigh Senger'
    """
    if pd.isna(desc):
        return ""

    cleaned = str(desc).strip()
    # remove ALL " and ' characters
    cleaned = cleaned.replace('"', "").replace("'", "")
    # collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


# ---------- Loaders ---------- #

def load_campaign_data(path: Path) -> pd.DataFrame:
    """
    Load and clean campaign_data.csv (raw).

    Handles both:
      - weird "single column with tabs" format, or
      - already columnar CSV with campaign_description + discount.

    Final columns:
      - campaign_id
      - campaign_name
      - campaign_description (quotes removed)
      - discount (numeric, from messy strings)
    """
    df_raw = pd.read_csv(path)

    # If file is in the weird "single column with tabs" format
    if len(df_raw.columns) == 1:
        col = df_raw.columns[0]
        parts = df_raw[col].astype(str).str.split("\t", n=4, expand=True)

        df = pd.DataFrame({
            "campaign_id": parts[1].astype(str),
            "campaign_name": parts[2].astype(str),
            "campaign_description_raw": parts[3].astype(str),
            "discount_raw": parts[4].astype(str),
        })
    else:
        # Already columnar, just rename expected columns if present
        df = df_raw.rename(columns={
            "campaign_description": "campaign_description_raw",
            "discount": "discount_raw",
        })

    # Clean description and discount
    df["campaign_description"] = df["campaign_description_raw"].apply(
        clean_description
    )
    df["discount"] = df["discount_raw"].apply(normalize_discount)

    # Try to store as nullable integer if possible
    try:
        df["discount"] = df["discount"].astype("Int64")
    except Exception:
        # If some are non-integers, just leave as numeric
        df["discount"] = pd.to_numeric(df["discount"], errors="coerce")

    # Final columns
    df_out = df[["campaign_id", "campaign_name",
                 "campaign_description", "discount"]].copy()

    # Enforce basic types
    df_out["campaign_id"] = df_out["campaign_id"].astype(str)
    df_out["campaign_name"] = df_out["campaign_name"].astype(str)
    df_out["campaign_description"] = df_out["campaign_description"].astype(str)

    return df_out


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
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce")

    # Keep raw string + numeric days
    df["estimated_arrival_raw"] = df["estimated arrival"].astype(str)
    df["estimated_arrival_days"] = df["estimated_arrival_raw"].apply(
        parse_estimated_arrival_days
    )

    # Make column names warehouse-friendly
    df = df.drop(columns=["estimated arrival"])
    # availed already int 0/1, keep as-is

    return df


# ---------- Split & Save ---------- #

def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Split into clean rows vs issue rows.

    - Issues = duplicates (by key_cols) OR any null/NaT
    - Formatting issues (description quotes, discount strings) are already
      handled in load_campaign_data, so they are NOT treated as issues here.
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


# ---------- main ---------- #

def main():
    print("=== Ingesting Marketing department datasets ===\n")

    # ---------- campaign_data ---------- #
    print("Loading campaign_data.csv ...")
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
