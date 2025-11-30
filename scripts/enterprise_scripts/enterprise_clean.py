# Cleaning Script for Enterprise Department Tables

import pandas as pd
from pathlib import Path
import re

# ================== CONFIG ================== #
# Get Root:
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
RAW_DIR = PROJECT_ROOT / "data_files" / "Enterprise Department"

# Raw/Input Files:
MERCHANT_FILE = RAW_DIR / "merchant_data.csv"
STAFF_FILE = RAW_DIR / "staff_data.csv"
ORDER_FILES = [
    RAW_DIR / "order_with_merchant_data1.csv",
    RAW_DIR / "order_with_merchant_data2.csv",
    RAW_DIR / "order_with_merchant_data3.csv",
]

# Outputs
OUT_DIR = Path("clean_data") / "enterprise"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


# ---------- shared helpers ---------- #

def normalize_phone(phone: str) -> str:
    """
    Normalize phone numbers to a consistent format.

    - Keep only digits.
    - If 10 digits  -> XXX-XXX-XXXX
    - If 11 digits starting with '1' -> drop leading 1, format as above
    - Otherwise -> return digits string as-is (still consistent)
    """
    if pd.isna(phone):
        return ""

    digits = re.sub(r"\D", "", str(phone))

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

    return digits


def clean_merchant_name(name: str) -> str:
    """
    Remove inconsistent outer quotation marks and extra spaces.
    E.g.  '"Ontodia, Inc"' -> 'Ontodia, Inc'
          '  "Whitby Group"  ' -> 'Whitby Group'
    """
    if pd.isna(name):
        return ""

    cleaned = str(name).strip()
    cleaned = cleaned.strip('"').strip("'")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _digits_from_id(val) -> str:
    """Extract just the digits from an ID, e.g. 'MERCHANT10101' -> '10101'."""
    if pd.isna(val):
        return ""
    s = str(val)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or s  # fallback to whole string if no digits


def assign_ids_and_keys(df: pd.DataFrame, id_col: str, key_col: str) -> pd.DataFrame:
    """
    Create a surrogate key per row based on the business ID, without changing the ID.

    For each group of the same id_col:
      - preserve original row order
      - seq = 1..N

    Example:
      merchant_id = MERCHANT42692 appears twice
      -> merchant_key: 42692-1, 42692-2
      (merchant_id itself stays MERCHANT42692 for both rows)
    """
    if id_col not in df.columns:
        return df

    df = df.copy()

    # Remember original ID (for reference) and row order
    orig_col = f"{id_col}_orig"
    df[orig_col] = df[id_col].astype(str)
    df["_orig_index"] = df.index

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        # preserve original order within the group
        group = group.sort_values("_orig_index").copy()

        base_id = group[id_col].iloc[0]
        digits = _digits_from_id(base_id)

        group["seq"] = range(1, len(group) + 1)

        # merchant_key / staff_key based on digits + seq
        group[key_col] = [f"{digits}-{i}" for i in group["seq"]]

        return group

    # groupby with sort=False so groups follow first appearance
    df = df.groupby(id_col, sort=False, group_keys=False).apply(process_group)

    # Restore original row order and drop helper cols (keep *_orig)
    df = df.sort_values("_orig_index").drop(columns=["seq", "_orig_index"])

    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Split into clean rows vs issue rows.

    - Issues = duplicates (by key_cols) OR any null/NaT
    - Formatting problems (phones, merchant name quotes) are already fixed,
      so they are NOT treated as issues.
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

    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows:  {len(clean_df):,}")
    print(f"  Issue rows:  {len(issues_df):,}")
    print(f"  Clean CSV:   {csv_path}")
    print(f"  Parquet:     {parquet_path}")
    print(f"  Issues CSV:  {issues_path}\n")


# ---------- loaders with cleaning built-in ---------- #

def load_merchant_data(path: Path) -> pd.DataFrame:
    """Load raw merchant_data.csv and clean names + phone numbers."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    if "merchant_id" in df.columns:
        df["merchant_id"] = df["merchant_id"].astype(str)

    if "name" in df.columns:
        df["name"] = df["name"].astype(str).apply(clean_merchant_name)

    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str)
        df["contact_number"] = df["contact_number"].apply(normalize_phone)

    # force any country column to 'United States'
    if "country" in df.columns:
        df["country"] = "United States"

    return df


def load_staff_data(path: Path) -> pd.DataFrame:
    """Load raw staff_data.csv and clean phone numbers."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    for col in ["staff_id", "name"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str)
        df["contact_number"] = df["contact_number"].apply(normalize_phone)

    # force any country column to 'United States'
    if "country" in df.columns:
        df["country"] = "United States"

    return df


def load_orders_with_merchant_data(paths) -> pd.DataFrame:
    """Load and concatenate order_with_merchant_data1/2/3.csv"""
    frames = []
    for p in paths:
        df = pd.read_csv(p)
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined


# ---------- main pipeline ---------- #

def main():
    print("=== Ingesting Enterprise datasets (combined) ===\n")

    # ---------- merchant_data ---------- #
    print("Loading merchant_data.csv ...")
    merchant_raw = load_merchant_data(MERCHANT_FILE)

    # Assign merchant_key (handles duplicates but does NOT change merchant_id)
    merchant_with_keys = assign_ids_and_keys(
        merchant_raw, id_col="merchant_id", key_col="merchant_key"
    )

    print(merchant_with_keys.head(), "\n")
    print(merchant_with_keys.dtypes, "\n")

    # Use merchant_key as uniqueness key
    merchant_clean, merchant_issues = split_clean_and_issues(
        merchant_with_keys, key_cols=["merchant_key"]
    )
    save_outputs(merchant_clean, merchant_issues, "merchant_data")

    # ---------- staff_data ---------- #
    print("Loading staff_data.csv ...")
    staff_raw = load_staff_data(STAFF_FILE)

    # Assign staff_key (handles duplicates but does NOT change staff_id)
    staff_with_keys = assign_ids_and_keys(
        staff_raw, id_col="staff_id", key_col="staff_key"
    )

    print(staff_with_keys.head(), "\n")
    print(staff_with_keys.dtypes, "\n")

    # Use staff_key as uniqueness key
    staff_clean, staff_issues = split_clean_and_issues(
        staff_with_keys, key_cols=["staff_key"]
    )
    save_outputs(staff_clean, staff_issues, "staff_data")

    # ---------- order_with_merchant_data1/2/3 ---------- #
    print("Loading order_with_merchant_data1/2/3.csv ...")
    orders_raw = load_orders_with_merchant_data(ORDER_FILES)
    print(orders_raw.head(), "\n")
    print(orders_raw.dtypes, "\n")

    order_key_cols = ["order_id"] if "order_id" in orders_raw.columns else None
    orders_clean, orders_issues = split_clean_and_issues(
        orders_raw, key_cols=order_key_cols
    )
    save_outputs(orders_clean, orders_issues, "order_with_merchant_data_all")

    print("Enterprise ingestion complete ✅")


if __name__ == "__main__":
    main()
