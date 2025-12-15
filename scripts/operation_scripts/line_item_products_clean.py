# ============================================================
# Cleaning Script — Operations / Line Item Products
# Purpose: Prepare product references for line-item fact source
# Layer: Cleaning (NO joins, NO surrogate keys)
# ============================================================

import pandas as pd
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

RAW_DIR = Path("/data_files/Operations Department")
OUT_DIR = Path("/clean_data/operations")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRODUCT_FILES = sorted(RAW_DIR.glob("line_item_data_products*.csv"))

# ============================================================
# HELPERS
# ============================================================

def load_and_concat(files):
    if not files:
        raise FileNotFoundError(
            "❌ No line_item_data_products*.csv files found"
        )

    frames = []
    for f in files:
        df = pd.read_csv(f)

        # Remove pandas artifact
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def split_clean_and_issues(df: pd.DataFrame):
    """
    Required for a valid line-item product row:
    - order_id
    - product_id
    - product_name
    """
    required = ["order_id", "product_id", "product_name"]

    issue_mask = df[required].isna().any(axis=1)

    clean_df = df.loc[~issue_mask].copy()
    issues_df = df.loc[issue_mask].copy()

    return clean_df, issues_df


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    clean_df.to_parquet(OUT_DIR / f"{name}.parquet", index=False)

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
    print("\n=== Cleaning Line Item Product Data ===\n")

    df = load_and_concat(PRODUCT_FILES)

    # Normalize fields
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["product_name"] = df["product_name"].astype(str).str.strip()

    # Sort for deterministic downstream joins
    df = df.sort_values(["order_id", "product_id", "product_name"])

    clean_df, issues_df = split_clean_and_issues(df)

    save_outputs(clean_df, issues_df, "line_item_products_clean")

    print("\nLine item product cleaning completed ✓\n")


if __name__ == "__main__":
    main()
