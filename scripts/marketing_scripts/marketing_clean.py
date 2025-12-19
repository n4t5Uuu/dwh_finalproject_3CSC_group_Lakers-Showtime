# Cleaning Script for Marketing – Transactional Campaign + Campaign Data

import re
from pathlib import Path
import pandas as pd

# ================== CONFIG ================== #
SCRIPT_DIR = Path(__file__).resolve().parent
RAW_DIR = Path("/data_files/Marketing Department")


CAMPAIGN_FILE = RAW_DIR / "campaign_data.csv"
TXN_CAMPAIGN_FILE = RAW_DIR / "transactional_campaign_data.csv"

OUT_DIR = Path("/clean_data/marketing")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ================== CLEANING ================== #

def to_date_key(dt: pd.Series) -> pd.Series:
    """
    Convert datetime series to int YYYYMMDD.
    NaT -> <NA>.
    """
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def normalize_discount(val):
    """Normalize discount strings into numeric."""
    if pd.isna(val):
        return pd.NA
    text = str(val).strip().lower()
    m = re.search(r"(\d+(\.\d+)?)", text)
    if not m:
        return pd.NA
    num = float(m.group(1))
    return int(num) if num.is_integer() else num


def clean_description(desc):
    """Remove all quote marks and collapse spaces."""
    if pd.isna(desc):
        return ""
    cleaned = str(desc).replace('"', "").replace("'", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned



def load_campaign_data(path: Path) -> pd.DataFrame:
    df_raw = pd.read_csv(path, header=None)

    # If single-column malformed data
    if df_raw.shape[1] == 1:
        df_raw = df_raw[0].astype(str).str.split("\t", expand=True)

    # Build cleaned DataFrame
    df = pd.DataFrame({
        "campaign_id": df_raw[1].astype(str),
        "campaign_name": df_raw[2].astype(str),
        "campaign_description": df_raw[3].astype(str).apply(clean_description),
        "discount_pct": df_raw[4].astype(str).apply(normalize_discount),
    })

    # Try to convert to nullable integer
    try:
        df["discount_pct"] = df["discount_pct"].astype("Int64")
    except:
        df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    # Remove garbage rows (non-campaign)
    df = df[df["campaign_id"].str.contains("CAMPAIGN", na=False)]

    # Business rule: keep the last occurrence of each campaign_id
    df = df.drop_duplicates(subset=["campaign_id"], keep="last").reset_index(drop=True)

    # ➤ ADD SURROGATE KEY
    df["campaign_key"] = df["campaign_id"].astype(str) + "-1"

    return df



def parse_estimated_arrival_days(text):
    if pd.isna(text):
        return None
    m = re.search(r"(\d+)", str(text))
    return int(m.group(1)) if m else None


def load_transactional_campaign_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["campaign_id"] = df["campaign_id"].astype(str)
    df["order_id"] = df["order_id"].astype(str)

    # Parse transaction_date 
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Add date key 
    df["transaction_date_key"] = to_date_key(df["transaction_date"])

    # Parse estimated arrival
    df["estimated_arrival_days"] = (
        df["estimated arrival"].astype(str).apply(parse_estimated_arrival_days)
    )

    df = df.drop(columns=["estimated arrival"])

    return df



# ---------------------------------------------------
# SPLIT CLEAN / ISSUES
# ---------------------------------------------------

def split_clean_and_issues(df, key_cols=None):
    
    null_mask = df.isna().any(axis=1)
    issues_df = df.loc[null_mask].copy()
    clean_df = df.loc[~null_mask].copy()
    return clean_df, issues_df


def save_outputs(clean_df, issues_df, name):
    csv_path = OUT_DIR / f"{name}.csv"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    clean_df.to_csv(csv_path, index=False)

    issues_df = issues_df.copy().fillna("")
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}: clean={len(clean_df)}, issues={len(issues_df)}")




def main():
    print("\n=== Processing Marketing Department ===\n")

    # CAMPAIGN DATA
    print("Loading campaign_data.csv ...")
    campaigns = load_campaign_data(CAMPAIGN_FILE)
    campaigns_clean, campaigns_issues = split_clean_and_issues(campaigns)
    save_outputs(campaigns_clean, campaigns_issues, "campaign_data")

    # TRANSACTIONAL CAMPAIGN DATA
    print("Loading transactional_campaign_data.csv ...")
    txn_df = load_transactional_campaign_data(TXN_CAMPAIGN_FILE)
    txn_clean, txn_issues = split_clean_and_issues(
        txn_df, key_cols=["order_id", "campaign_id"]
    )
    save_outputs(txn_clean, txn_issues, "transactional_campaign_data")

    print("\nMarketing ingestion complete \n")


if __name__ == "__main__":
    main()
