# step_enterprise_ingest.py

import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #
RAW_DIR = Path("/data_files") / "Enterprise Department"  # folder where your CSVs are located

MERCHANT_FILE = RAW_DIR / "merchant_data.csv"
STAFF_FILE = RAW_DIR / "staff_data.csv"
ORDER_FILES = [
    RAW_DIR / "order_with_merchant_data1.csv",
    RAW_DIR / "order_with_merchant_data2.csv",
    RAW_DIR / "order_with_merchant_data3.csv",
]

OUT_DIR = Path("/clean_data") / "enterprise"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


def load_merchant_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean merchant_data.csv"""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Enforce common types if columns exist
    for col in ["merchant_id", "name", "contact_number"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df


def load_staff_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean staff_data.csv"""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Enforce common types if columns exist
    for col in ["staff_id", "name", "contact_number"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

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


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Split into clean rows vs issue rows.

    - Issues = duplicates (by key_cols) OR any null/NaT
    - Formatting problems (phone style, extra quotes) are NOT treated as issues.
      Those will be fixed in separate clean scripts.
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
      - clean CSV + Parquet as {name}_clean.csv / {name}.parquet
      - issues CSV as {name}_issues.csv
    """
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    # Clean data
    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    # Issues: blanks instead of NaN/NaT for easier manual review
    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows:  {len(clean_df):,}")
    print(f"  Issue rows:  {len(issues_df):,}")
    print(f"  Clean CSV:   {csv_path}")
    print(f"  Parquet:     {parquet_path}")
    print(f"  Issues CSV:  {issues_path}\n")


def main():
    print("=== Ingesting Enterprise datasets ===\n")

    # ---------- merchant_data ---------- #
    print("Loading merchant_data.csv ...")
    merchant_raw = load_merchant_data(MERCHANT_FILE)
    print(merchant_raw.head(), "\n")
    print(merchant_raw.dtypes, "\n")

    merchant_key_cols = ["merchant_id"] if "merchant_id" in merchant_raw.columns else None
    merchant_clean, merchant_issues = split_clean_and_issues(
        merchant_raw, key_cols=merchant_key_cols
    )
    save_outputs(merchant_clean, merchant_issues, "merchant_data")

    # ---------- staff_data ---------- #
    print("Loading staff_data.csv ...")
    staff_raw = load_staff_data(STAFF_FILE)
    print(staff_raw.head(), "\n")
    print(staff_raw.dtypes, "\n")

    staff_key_cols = ["staff_id"] if "staff_id" in staff_raw.columns else None
    staff_clean, staff_issues = split_clean_and_issues(
        staff_raw, key_cols=staff_key_cols
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