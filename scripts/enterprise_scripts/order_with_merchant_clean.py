# Cleaning Script for Enterprise – Order–Merchant–Staff 

import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #

RAW_DIR = Path("/data_files/Enterprise Department")
OUT_DIR = Path("/clean_data/enterprise")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ORDER_MERCHANT_FILES = sorted(
    RAW_DIR.glob("order_with_merchant_data*.csv")
)

# ================== CLEANING ================== #

def load_and_concat(files):
    if not files:
        raise FileNotFoundError(
            "No order_with_merchant_data*.csv files found"
        )

    frames = []
    for f in files:
        df = pd.read_csv(f)

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def split_clean_and_issues(df: pd.DataFrame):
    """
    Required:
    - order_id
    - merchant_id
    - staff_id
    """
    required = ["order_id", "merchant_id", "staff_id"]

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
    print("\n=== Cleaning Order–Merchant–Staff Mapping ===\n")

    df = load_and_concat(ORDER_MERCHANT_FILES)

    # Normalize identifiers
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["merchant_id"] = df["merchant_id"].astype(str).str.strip()
    df["staff_id"] = df["staff_id"].astype(str).str.strip()

    # Enforce one row per order (latest wins if duplicates exist)
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    # Sort for deterministic downstream joins
    df = df.sort_values("order_id")

    clean_df, issues_df = split_clean_and_issues(df)

    save_outputs(clean_df, issues_df, "order_with_merchant_clean")

    print("\nOrder–Merchant–Staff cleaning completed ✓\n")


if __name__ == "__main__":
    main()
