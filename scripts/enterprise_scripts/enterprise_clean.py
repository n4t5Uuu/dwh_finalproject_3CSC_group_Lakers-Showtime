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


def normalize_merchant_id(val: str) -> str:
    """
    Ensure merchant IDs have 5-digit numeric part, e.g.:

      MERCHANT2347  -> MERCHANT02347
      MERCHANT1931  -> MERCHANT01931
      MERCHANT42692 -> MERCHANT42692 (unchanged)

    Keeps whatever prefix exists (normally 'MERCHANT').
    """
    if pd.isna(val):
        return ""

    s = str(val).strip()
    m = re.match(r"^(\D*)(\d+)$", s)
    if not m:
        # if it doesn't match PREFIX+digits, just return as-is
        return s

    prefix, digits = m.group(1), m.group(2)
    digits = digits.zfill(5)  # pad to 5 digits
    return prefix + digits


def assign_ids_and_keys(df: pd.DataFrame, id_col: str, key_col: str) -> pd.DataFrame:
    """
    Create a surrogate key per row based on the business ID, WITHOUT changing the ID.

    For each group of the same id_col:
      - preserve original row order
      - seq = 1..N

    Example (merchant):
      merchant_id = MERCHANT42692 appears twice
      -> merchant_key: MERCHANT42692-1, MERCHANT42692-2

    Example (staff):
      staff_id = STAFF010101 appears twice
      -> staff_key: STAFF010101-1, STAFF010101-2
    """
    if id_col not in df.columns:
        return df

    df = df.copy()
    df[id_col] = df[id_col].astype(str)
    df["_orig_index"] = df.index

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("_orig_index").copy()

        base_id = group[id_col].iloc[0]  # e.g., MERCHANT42692
        group["seq"] = range(1, len(group) + 1)

        # Key keeps prefix + ID, plus -1, -2, ...
        group[key_col] = [f"{base_id}-{i}" for i in group["seq"]]
        return group

    df = df.groupby(id_col, sort=False, group_keys=False).apply(process_group)

    df = df.sort_values("_orig_index").drop(columns=["seq", "_orig_index"])
    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Split into clean rows vs issue rows.

    - Issues = duplicates (by key_cols) OR any null/NaT
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
    """Load raw merchant_data.csv and clean IDs, names, phone numbers."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    if "merchant_id" in df.columns:
        df["merchant_id"] = df["merchant_id"].astype(
            str).apply(normalize_merchant_id)

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


def attach_keys_to_orders(
    orders_df: pd.DataFrame,
    merchant_df: pd.DataFrame,
    staff_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach merchant_key and staff_key to orders, then drop raw IDs.

    - Keep order_id.
    - Use merchant_key / staff_key instead of merchant_id / staff_id.
    """
    df = orders_df.copy()

    # Merchant mapping
    if "merchant_id" in df.columns and "merchant_id" in merchant_df.columns:
        merchant_map = (
            merchant_df[["merchant_id", "merchant_key"]]
            .drop_duplicates(subset=["merchant_id"])
        )
        df = df.merge(merchant_map, on="merchant_id", how="left")

    # Staff mapping
    if "staff_id" in df.columns and "staff_id" in staff_df.columns:
        staff_map = (
            staff_df[["staff_id", "staff_key"]]
            .drop_duplicates(subset=["staff_id"])
        )
        df = df.merge(staff_map, on="staff_id", how="left")

    # Drop the raw IDs; keep keys + order_id + other columns
    for col in ["merchant_id", "staff_id"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    return df


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

    # Attach merchant_key + staff_key and drop merchant_id / staff_id
    orders_with_keys = attach_keys_to_orders(
        orders_raw, merchant_with_keys, staff_with_keys
    )

    print(orders_with_keys.head(), "\n")
    print(orders_with_keys.dtypes, "\n")

    # Issues based on order_id (keys are just attributes here)
    order_key_cols = [
        "order_id"] if "order_id" in orders_with_keys.columns else None
    orders_clean, orders_issues = split_clean_and_issues(
        orders_with_keys, key_cols=order_key_cols
    )
    save_outputs(orders_clean, orders_issues, "order_with_merchant_data_all")

    print("Enterprise ingestion complete ✅")


if __name__ == "__main__":
    main()
