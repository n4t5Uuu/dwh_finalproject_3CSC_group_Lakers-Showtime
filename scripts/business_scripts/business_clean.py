# Cleaning Script for Business Department – Product List

import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #

INPUT_FILE = Path("/data_files/Business Department/product_list.csv")
OUTPUT_DIR = Path("/clean_data/business")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_CSV = OUTPUT_DIR / "product_list_clean.csv"
ISSUES_CSV = OUTPUT_DIR / "product_list_issues.csv"

# ================== CLEANING ================== #

def main():
    print(f"Reading raw product file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    # Drop auto-generated index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Enforce schema
    df["product_id"] = df["product_id"].astype(str)
    df["product_name"] = df["product_name"].astype(str)
    df["product_type"] = df["product_type"].astype(str)

    # Fix known category issues
    df.loc[df["product_type"] == "toolss", "product_type"] = "tools"
    df.loc[df["product_type"] == "cosmetic", "product_type"] = "cosmetics"

    # Price must be numeric
    df["price_numeric"] = pd.to_numeric(df["price"], errors="coerce")

    # Identify issue rows
    null_mask = df[["product_id", "product_name", "product_type", "price"]].isna().any(axis=1)
    invalid_price_mask = df["price_numeric"].isna()
    
    # Fix missing category for known product
    mask_bottle_paint = (
        df["product_name"].astype(str)
        .str.contains("bottle of paint", case=False, na=False)
        & df["product_type"].isna()
    )

    df.loc[mask_bottle_paint, "product_type"] = "stationary and school supplies"


    issue_mask = null_mask | invalid_price_mask

    issues = df.loc[issue_mask].copy().fillna("")
    clean = df.loc[~issue_mask].copy()

    # Enforce final types
    clean["price"] = clean["price_numeric"].astype(float)
    clean = clean.drop(columns=["price_numeric"])

    # Save outputs
    clean.to_csv(CLEAN_CSV, index=False)
    issues.drop(columns=["price_numeric"], errors="ignore").to_csv(ISSUES_CSV, index=False)

    print("Business product cleaning complete")
    print(f"Total rows: {len(df):,}")
    print(f"Clean rows: {len(clean):,}")
    print(f"Issue rows: {len(issues):,}")
    print(f"Clean file: {CLEAN_CSV}")
    print(f"Issues file: {ISSUES_CSV}")

if __name__ == "__main__":
    main()
