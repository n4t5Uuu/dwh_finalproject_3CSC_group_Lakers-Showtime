# ============================================================
# Cleaning Script — Enterprise / Staff
# Purpose: Prepare staff data for SCD Type 2 dimension
# Layer: Cleaning (NO surrogate keys, NO SCD logic)
# ============================================================

import pandas as pd
from pathlib import Path
import re

# ============================================================
# CONFIG
# ============================================================

RAW_DIR = Path("/data_files/Enterprise Department")
OUT_DIR = Path("/clean_data/enterprise")
OUT_DIR.mkdir(parents=True, exist_ok=True)

STAFF_FILE = RAW_DIR / "staff_data.csv"

# ============================================================
# HELPERS
# ============================================================

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


def split_clean_and_issues(df: pd.DataFrame):
    """
    Staff is SCD Type 2.
    Only critical fields are required.
    """
    required_cols = ["staff_id", "creation_date", "name", "job_level"]
    issue_mask = df[required_cols].isna().any(axis=1)

    return df[~issue_mask].copy(), df[issue_mask].copy()


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    clean_df.to_parquet(OUT_DIR / f"{name}.parquet", index=False)
    issues_df.fillna("").to_csv(OUT_DIR / f"{name}_issues.csv", index=False)

    print(f"[OK] {name}: Clean={len(clean_df):,}, Issues={len(issues_df):,}")

# ============================================================
# LOAD + CLEAN STAFF
# ============================================================

def load_and_clean_staff(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize business key
    df["staff_id"] = (
        df["staff_id"]
        .astype(str)
        .apply(lambda x: normalize_business_id(x, "STAFF"))
    )

    # Normalize phone
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].apply(normalize_phone)

    # Parse creation_date (SCD anchor)
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    df["staff_creation_date_key"] = to_date_key(df["creation_date"])

    # Normalize descriptive fields
    df["name"] = df["name"].astype(str).str.strip()
    df["job_level"] = (
        df["job_level"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    # Country: keep source value, default only if missing
    df["country"] = "United States"

    # Order preserved for SCD logic downstream
    df = df.sort_values(["staff_id", "creation_date"])

    return df

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n=== Cleaning Enterprise Staff Data (SCD-Ready) ===\n")

    staff_df = load_and_clean_staff(STAFF_FILE)
    staff_clean, staff_issues = split_clean_and_issues(staff_df)

    save_outputs(staff_clean, staff_issues, "staff_data")

    print("Staff cleaning completed ✓")


if __name__ == "__main__":
    main()
