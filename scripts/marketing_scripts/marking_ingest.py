# step_marketing_ingest.py

import re
from pathlib import Path

import pandas as pd


RAW_DIR = Path(".")  

CAMPAIGN_FILE = RAW_DIR / "campaign_data.csv"
TXN_CAMPAIGN_FILE = RAW_DIR / "transactional_campaign_data.csv"

OUT_DIR = Path("ingested") / "marketing"
OUT_DIR.mkdir(parents=True, exist_ok=True)


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
    campaign_data.csv is messed up: everything is in one column,
    and the header is like: '\\tcampaign_id\\tcampaign_name\\tcampaign_description\\tdiscount'

    We'll:
    - read as-is
    - split that single column on '\t'
    - build proper columns
    - parse discount to numeric percent
    """
    raw = pd.read_csv(path)

    single_col_name = raw.columns[0]

    parts = raw[single_col_name].astype(str).str.split("\t", n=4, expand=True)

    df = pd.DataFrame(
        {
            "campaign_id": parts[1].astype(str),
            "campaign_name": parts[2].astype(str),
            "campaign_description": parts[3].astype(str),
            "discount_raw": parts[4].astype(str),
        }
    )

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

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["campaign_id"] = df["campaign_id"].astype(str)
    df["order_id"] = df["order_id"].astype(str)

    # Parse date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Keep raw string + numeric days
    df["estimated_arrival_raw"] = df["estimated arrival"].astype(str)
    df["estimated_arrival_days"] = df["estimated_arrival_raw"].apply(
        parse_estimated_arrival_days
    )

    df = df.drop(columns=["estimated arrival"])

    return df


def save_outputs(df: pd.DataFrame, name: str):
    """Save both CSV and Parquet with a consistent naming pattern."""
    csv_path = OUT_DIR / f"{name}_clean.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"Saved {name}: {len(df):,} rows")
    print(f"  CSV:     {csv_path}")
    print(f"  Parquet: {parquet_path}\n")


def main():
    print("=== Ingesting Marketing department datasets ===\n")

    # campaign_data
    print("Loading campaign_data.csv ...")
    campaigns = load_campaign_data(CAMPAIGN_FILE)
    print(campaigns.head())
    print(campaigns.dtypes, "\n")
    save_outputs(campaigns, "campaign_data")

    # transactional_campaign_data
    print("Loading transactional_campaign_data.csv ...")
    txn_campaigns = load_transactional_campaign_data(TXN_CAMPAIGN_FILE)
    print(txn_campaigns.head())
    print(txn_campaigns.dtypes, "\n")
    save_outputs(txn_campaigns, "transactional_campaign_data")

    print("Marketing ingestion complete ✅")


if __name__ == "__main__":
    main()