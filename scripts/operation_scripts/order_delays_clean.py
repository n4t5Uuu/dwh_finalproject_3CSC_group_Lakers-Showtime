# ============================================================
# Cleaning Script — Operations / Order Delays
# Purpose: Prepare optional delay measure for orders
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

DELAY_FILE = RAW_DIR / "order_delays.csv"

# ============================================================
# CLEANING LOGIC
# ============================================================

def split_clean_and_issues(df: pd.DataFrame):
    """
    Required fields:
    - order_id
    - delay_in_days
    """
    required = ["order_id", "delay_in_days"]

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
    print("\n=== Cleaning Order Delays ===\n")

    df = pd.read_csv(DELAY_FILE)

    # Drop pandas artifact
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize
    df["order_id"] = df["order_id"].astype(str).str.strip()

    df = df.rename(columns={
        "delay in days": "delay_in_days"
    })

    df["delay_in_days"] = pd.to_numeric(
        df["delay_in_days"], errors="coerce"
    )

    # Sort for deterministic joins later
    df = df.sort_values("order_id")

    clean_df, issues_df = split_clean_and_issues(df)

    save_outputs(clean_df, issues_df, "order_delays_clean")

    print("\nOrder delays cleaning completed ✓\n")


if __name__ == "__main__":
    main()
