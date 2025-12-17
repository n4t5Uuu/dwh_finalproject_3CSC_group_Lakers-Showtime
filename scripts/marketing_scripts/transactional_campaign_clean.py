# ============================================================
# Cleaning Script — Marketing / Transactional Campaign
# Purpose: Prepare campaign transaction events
# Layer: Cleaning (NO joins, NO surrogate keys)
# ============================================================

import pandas as pd
import re
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

RAW_DIR = Path("/data_files/Marketing Department")
OUT_DIR = Path("/clean_data/marketing")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CAMPAIGN_TXN_FILE = RAW_DIR / "transactional_campaign_data.csv"

# ============================================================
# HELPERS
# ============================================================

def parse_days(val):
    """
    Extract integer days from strings like '10days'
    """
    if pd.isna(val):
        return pd.NA
    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else pd.NA


def to_date_key(dt: pd.Series) -> pd.Series:
    """
    Convert datetime to YYYYMMDD integer
    """
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def split_clean_and_issues(df: pd.DataFrame):
    """
    Required fields:
    - order_id
    - campaign_id
    - transaction_date
    - availed
    """
    required = [
        "order_id",
        "campaign_id",
        "transaction_date",
        "date_key",
        "availed",
    ]

    issue_mask = df[required].isna().any(axis=1)

    clean_df = df.loc[~issue_mask].copy()
    issues_df = df.loc[issue_mask].copy()

    return clean_df, issues_df


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)

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
    print("\n=== Cleaning Transactional Campaign Data ===\n")

    df = pd.read_csv(CAMPAIGN_TXN_FILE)

    # Drop pandas artifact
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize IDs
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["campaign_id"] = df["campaign_id"].astype(str).str.strip()

    # Parse transaction_date
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce"
    )

    df["date_key"] = to_date_key(df["transaction_date"])

    # Parse estimated arrival
    df["estimated_arrival_days"] = (
        df["estimated arrival"].apply(parse_days)
    )
    df = df.drop(columns=["estimated arrival"])

    # Normalize availed flag
    df["availed"] = pd.to_numeric(
        df["availed"], errors="coerce"
    ).astype("Int64")

    # Sort for deterministic joins
    df = df.sort_values(
        ["transaction_date", "order_id", "campaign_id"]
    )

    clean_df, issues_df = split_clean_and_issues(df)

    save_outputs(clean_df, issues_df, "transactional_campaign_clean")

    print("\nTransactional campaign cleaning completed \n")


if __name__ == "__main__":
    main()
