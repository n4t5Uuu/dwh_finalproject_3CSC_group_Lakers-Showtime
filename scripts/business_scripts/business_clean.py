# Cleaning Script for Business Department Tables

import pandas as pd
from pathlib import Path
import re

# ================== CONFIG ================== #
# Get Root:
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

# Raw/Input Files:
RAW_DIR = PROJECT_ROOT / "data_files" / "Business Department"
INPUT_FILE = RAW_DIR / "product_list.csv"

# Outputs:
OUT_DIR = Path("clean_data") / "customer_management"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUT_DIR / "product_list.csv"
OUTPUT_PARQUET = OUT_DIR / "product_list.parquet"
BAD_ROWS_CSV = OUT_DIR / "product_list_issues.csv"
# ================== CONFIG ================== #


def _digits_from_id(val) -> str:
    """Extract just the digits from an ID like 'PROD10101' -> '10101'."""
    if pd.isna(val):
        return ""
    s = str(val)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or s  # fallback to whole string if no digits


def assign_product_ids_and_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each product_id group:
      - preserve original row order
      - seq = 1..N

    product_id:
        - NEVER modified; stays exactly as in the CSV.

    product_key:
        - keeps the prefix (e.g., PRODUCT)
        - numeric part is zero-padded to 5 digits
        - then '-<seq>' per row in the group

    Examples:
      product_id = 'PRODUCT16794' (1 row)
        -> product_key: 'PRODUCT16794-1'

      product_id = 'PRODUCT6794' (2 rows)
        -> key base = 'PRODUCT06794'
        -> product_key: 'PRODUCT06794-1', 'PRODUCT06794-2'
    """
    if "product_id" not in df.columns:
        return df

    df = df.copy()
    df["product_id"] = df["product_id"].astype(str)
    df["_orig_index"] = df.index

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("_orig_index").copy()

        base_id = group["product_id"].iloc[0]  # e.g., PRODUCT16794

        # Split into prefix + digits
        m = re.match(r"^(\D*)(\d+)$", base_id)
        if m:
            prefix, digits = m.group(1), m.group(2)
            digits_padded = digits.zfill(5)  # ensure 5 digits
            key_base = prefix + digits_padded
        else:
            # if it doesn't match PREFIX+digits, just use whole ID
            key_base = base_id

        group["seq"] = range(1, len(group) + 1)
        group["product_key"] = [f"{key_base}-{i}" for i in group["seq"]]

        return group

    df = df.groupby("product_id", sort=False,
                    group_keys=False).apply(process_group)

    df = df.sort_values("_orig_index").drop(columns=["seq", "_orig_index"])
    return df


def main():
    print(f"Reading: {INPUT_FILE}")

    # Read raw CSV
    df = pd.read_csv(INPUT_FILE)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Detect common column names
    name_col = "product_name" if "product_name" in df.columns else (
        "name" if "name" in df.columns else None
    )
    type_col = "product_type" if "product_type" in df.columns else (
        "type" if "type" in df.columns else None
    )
    price_col = "price" if "price" in df.columns else (
        "product_price" if "product_price" in df.columns else None
    )

    # ---------- FIX KNOWN DATA ISSUES ---------- #
    if name_col and type_col:
        # 1) bottle of paint missing type
        mask_bottle_paint = (
            df[name_col].astype(str).str.contains(
                "bottle of paint", case=False, na=False)
            & df[type_col].isna()
        )
        df.loc[mask_bottle_paint, type_col] = "stationary and school supplies"

        # 2) typo categories: toolss -> tools, cosmetic -> cosmetics
        df[type_col] = df[type_col].astype(str)
        df.loc[df[type_col] == "toolss", type_col] = "tools"
        df.loc[df[type_col] == "cosmetic", type_col] = "cosmetics"

    print("Sample rows after raw load + fixes:")
    print(df.head())

    # ---------- ASSIGN PRODUCT KEYS (product_id unchanged) ---------- #
    if "product_id" in df.columns:
        df = assign_product_ids_and_keys(df)

    # ---------- FIND ISSUE ROWS ---------- #
    # NOTE: Duplicates are ALLOWED and should NOT be issues.
    # We only flag:
    #   - rows with missing values
    #   - rows with invalid price values
    # 1) (Optional) You can still compute duplicate_mask for debugging, but we won't use it:
    # if "product_id" in df.columns:
    #     duplicate_mask = df.duplicated(subset=["product_id"], keep=False)
    # else:
    #     duplicate_mask = df.duplicated(keep=False)

    # 2) Nulls in any column
    null_mask = df.isna().any(axis=1)

    # 3) Invalid numeric values in price (non-null but not convertible)
    if price_col:
        price_numeric = pd.to_numeric(df[price_col], errors="coerce")
        invalid_price_mask = price_numeric.isna() & df[price_col].notna()
    else:
        invalid_price_mask = pd.Series(False, index=df.index)

    # FINAL: Do NOT include duplicates in issues.
    bad_mask = null_mask | invalid_price_mask

    bad_rows = df.loc[bad_mask].copy()
    clean_df = df.loc[~bad_mask].copy()

    # For the issues file: show blanks instead of "NaN"
    bad_rows = bad_rows.fillna("")

    # ---------- ENFORCE TYPES ON CLEAN DF ONLY ---------- #
    if not clean_df.empty:
        if "product_id" in clean_df.columns:
            clean_df["product_id"] = clean_df["product_id"].astype(str)
        if name_col and name_col in clean_df.columns:
            clean_df[name_col] = clean_df[name_col].astype(str)
        if type_col and type_col in clean_df.columns:
            clean_df[type_col] = clean_df[type_col].astype(str)
        if price_col and price_col in clean_df.columns:
            clean_df[price_col] = pd.to_numeric(
                clean_df[price_col], errors="raise")

    print("\nColumn dtypes (clean data):")
    print(clean_df.dtypes)

    # ---------- SAVE OUTPUTS ---------- #
    clean_df.to_csv(OUTPUT_CSV, index=False)
    clean_df.to_parquet(OUTPUT_PARQUET, index=False)
    bad_rows.to_csv(BAD_ROWS_CSV, index=False)

    print(f"\nBusiness ingestion complete ✅")
    print(f"Total rows: {len(df):,}")
    print(f"Clean rows: {len(clean_df):,}")
    print(f"Issue rows: {len(bad_rows):,}")
    print(f"Saved clean CSV to:      {OUTPUT_CSV}")
    print(f"Saved clean Parquet to:  {OUTPUT_PARQUET}")
    print(f"Saved issue rows to:     {BAD_ROWS_CSV}")


if __name__ == "__main__":
    main()
