import pandas as pd
from pathlib import Path

# ------------- CONFIG ------------- #
INPUT_FILE = Path("/data_files/Business Department/product_list.csv")

OUTPUT_DIR = Path("/clean_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / "product_list.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "product_list.parquet"
BAD_ROWS_CSV = OUTPUT_DIR / "product_list_issues.csv"
# ------------- CONFIG ------------- #


def main():
    print(f"Reading: {INPUT_FILE}")

    # Read raw CSV
    df = pd.read_csv(INPUT_FILE)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    print("Sample rows after raw load:")
    print(df.head())

    # ---------- FIND ISSUE ROWS ---------- #
    # 1) Duplicates (across all columns)
    duplicate_mask = df.duplicated(
        subset=["product_id", "product_name"],
        keep=False
    )

    # 2) Nulls in any column (from original CSV)
    null_mask = df.isna().any(axis=1)

    # 3) Invalid numeric values in price (non-null but not convertible)
    price_numeric = pd.to_numeric(df["price"], errors="coerce")
    invalid_price_mask = price_numeric.isna() & df["price"].notna()

    # Any problematic row
    bad_mask = duplicate_mask | null_mask | invalid_price_mask

    bad_rows = df.loc[bad_mask].copy()
    clean_df = df.loc[~bad_mask].copy()

    # For the issues file: show blanks instead of "NaN"
    bad_rows = bad_rows.fillna("")

    # ---------- ENFORCE TYPES ON CLEAN DF ONLY ---------- #
    if not clean_df.empty:
        clean_df["product_id"] = clean_df["product_id"].astype(str)
        clean_df["product_name"] = clean_df["product_name"].astype(str)
        clean_df["product_type"] = clean_df["product_type"].astype(str)
        # Now this should not create NaNs; bad ones were already filtered out
        clean_df["price"] = pd.to_numeric(clean_df["price"], errors="raise")

    print("\nColumn dtypes (clean data):")
    print(clean_df.dtypes)

    # ---------- SAVE OUTPUTS ---------- #
    clean_df.to_csv(OUTPUT_CSV, index=False)
    clean_df.to_parquet(OUTPUT_PARQUET, index=False)
    bad_rows.to_csv(BAD_ROWS_CSV, index=False)

    print(f"\nIngestion complete ✅")
    print(f"Total rows: {len(df):,}")
    print(f"Clean rows: {len(clean_df):,}")
    print(f"Issue rows: {len(bad_rows):,}")
    print(f"Saved clean CSV to:      {OUTPUT_CSV}")
    print(f"Saved clean Parquet to:  {OUTPUT_PARQUET}")
    print(f"Saved issue rows to:     {BAD_ROWS_CSV}")


if __name__ == "__main__":
    main()