
# Cleaning Script — Operations / Line Item Prices

import pandas as pd
import re
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

RAW_DIR = Path("/data_files/Operations Department")
OUT_DIR = Path("/clean_data/operations")
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRICE_FILES = sorted(RAW_DIR.glob("line_item_data_prices*.csv"))

# ============================================================
# HELPERS
# ============================================================

def parse_quantity(val):
    """
    Extract integer quantity from strings like:
    '6px', '4pcs', '5pieces'
    """
    if pd.isna(val):
        return pd.NA

    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else pd.NA


def load_and_concat(files):
    if not files:
        raise FileNotFoundError(
            "❌ No line_item_data_prices*.csv files found"
        )

    frames = []
    for f in files:
        df = pd.read_csv(f)

        # Drop pandas artifact
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def split_clean_and_issues(df: pd.DataFrame):
    """
    Required fields for a valid price line:
    - order_id
    - unit_price
    - quantity
    """
    required = ["order_id", "unit_price", "quantity"]

    issue_mask = df[required].isna().any(axis=1)

    clean_df = df.loc[~issue_mask].copy()
    issues_df = df.loc[issue_mask].copy()

    return clean_df, issues_df


def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)

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
    print("\n=== Cleaning Line Item Price Data ===\n")

    df = load_and_concat(PRICE_FILES)

    # Normalize identifiers
    df["order_id"] = df["order_id"].astype(str).str.strip()

    # Rename for clarity (Kimball-style measure naming)
    df = df.rename(columns={"price": "unit_price"})

    # Ensure numeric price
    df["unit_price"] = pd.to_numeric(
        df["unit_price"], errors="coerce"
    )

    # Parse quantity
    df["quantity"] = df["quantity"].apply(parse_quantity)

    # Sort for deterministic downstream joins
    df = df.sort_values(["order_id"])

    clean_df, issues_df = split_clean_and_issues(df)

    save_outputs(clean_df, issues_df, "line_item_prices_clean")

    print("\nLine item price cleaning completed ✓\n")


if __name__ == "__main__":
    main()
