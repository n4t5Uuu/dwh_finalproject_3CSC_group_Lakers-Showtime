
# Cleaning Script for Enterprise – Merchant Data

import pandas as pd
from pathlib import Path
import re


# ================== CONFIG ================== #
RAW_DIR = Path("/data_files/Enterprise Department")
OUT_DIR = Path("/clean_data/enterprise")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MERCHANT_FILE = RAW_DIR / "merchant_data.csv"

# ================== CLEANING ================== #

def to_date_key(dt: pd.Series) -> pd.Series:
    """Convert datetime series to int YYYYMMDD."""
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def normalize_phone(phone: str) -> str:
    if pd.isna(phone):
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return digits


def normalize_business_id(raw_id: str, prefix: str) -> str:
    if pd.isna(raw_id):
        return ""
    s = str(raw_id).strip()
    m = re.match(rf"^{prefix}(\d+)$", s)
    if not m:
        return s
    return f"{prefix}{m.group(1).zfill(5)}"


def clean_merchant_name(name: str) -> str:
    if pd.isna(name):
        return ""
    cleaned = str(name).strip().strip('"').strip("'")
    return re.sub(r"\s+", " ", cleaned)


def split_clean_and_issues(df: pd.DataFrame):
    """
    Merchant is SCD Type 2.
    Duplicates by merchant_id are VALID.
    Only critical nulls are issues.
    """
    required_cols = ["merchant_id", "creation_date", "name"]
    issue_mask = df[required_cols].isna().any(axis=1)

    return df[~issue_mask].copy(), df[issue_mask].copy()


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    issues_df.fillna("").to_csv(OUT_DIR / f"{name}_issues.csv", index=False)

    print(f"[OK] {name}: Clean={len(clean_df):,}, Issues={len(issues_df):,}")

# ============================================================
# LOAD + CLEAN MERCHANT
# ============================================================

def load_and_clean_merchant(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Drop pandas index artifact
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize business key
    df["merchant_id"] = (
        df["merchant_id"]
        .astype(str)
        .apply(lambda x: normalize_business_id(x, "MERCHANT"))
    )

    # Clean merchant name
    df["name"] = df["name"].apply(clean_merchant_name)

    # Normalize phone
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].apply(normalize_phone)

    # Parse creation date 
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    df["merchant_creation_date_key"] = to_date_key(df["creation_date"])

    # Country normalization 
    df["country"] = "United States"

    # Sort for downstream SCD logic
    df = df.sort_values(["merchant_id", "creation_date"])

    return df

def main():
    print("\n=== Cleaning Enterprise Merchant Data (SCD-Ready) ===\n")

    merchant_df = load_and_clean_merchant(MERCHANT_FILE)
    merchant_clean, merchant_issues = split_clean_and_issues(merchant_df)

    save_outputs(merchant_clean, merchant_issues, "merchant_data")

    print("Merchant cleaning completed ✓")


if __name__ == "__main__":
    main()
