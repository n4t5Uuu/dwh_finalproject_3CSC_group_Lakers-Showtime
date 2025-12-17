# Cleaning Script for Marketing – Campaign Data

import re
from pathlib import Path
import pandas as pd

# ================== CONFIG ================== #

RAW_DIR = Path("/data_files/Marketing Department")
OUT_DIR = Path("/clean_data/marketing")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CAMPAIGN_FILE = RAW_DIR / "campaign_data.csv"

# ================== CLEANING ================== #

def normalize_discount(val):
    """
    Normalize discount strings like:
    '1%', '1pct', '1percent', '10%%' → integer percentage
    """
    if pd.isna(val):
        return pd.NA
    text = str(val).lower()
    m = re.search(r"(\d+(\.\d+)?)", text)
    if not m:
        return pd.NA
    return int(float(m.group(1)))


def clean_description(desc):
    """Remove quotes and collapse whitespace."""
    if pd.isna(desc):
        return ""
    cleaned = str(desc).replace('"', "").replace("'", "")
    return re.sub(r"\s+", " ", cleaned).strip()

# ============================================================
# LOAD + CLEAN CAMPAIGN DATA
# ============================================================

def load_and_clean_campaign(path: Path) -> pd.DataFrame:
    df_raw = pd.read_csv(path, header=None)

    # Handle malformed tab-separated rows
    if df_raw.shape[1] == 1:
        df_raw = df_raw[0].astype(str).str.split("\t", expand=True)

    df = pd.DataFrame({
        "campaign_id": df_raw[1].astype(str),
        "campaign_name": df_raw[2].astype(str).str.strip(),
        "campaign_description": df_raw[3].astype(str).apply(clean_description),
        "discount_pct": df_raw[4].astype(str).apply(normalize_discount),
    })

    # Keep valid campaign rows only
    df = df[df["campaign_id"].str.contains("CAMPAIGN", na=False)]

    # Discount as nullable integer
    df["discount_pct"] = df["discount_pct"].astype("Int64")

    # Type 1 rule: keep latest occurrence
    df = df.drop_duplicates(subset=["campaign_id"], keep="last")

    return df.reset_index(drop=True)

# ============================================================
# SPLIT CLEAN / ISSUES
# ============================================================

def split_clean_and_issues(df: pd.DataFrame):
    """
    Issues = missing required dimension attributes
    """
    required_cols = ["campaign_id", "campaign_name", "discount_pct"]
    issue_mask = df[required_cols].isna().any(axis=1)

    return df[~issue_mask].copy(), df[issue_mask].copy()

# ============================================================
# SAVE OUTPUTS
# ============================================================

def save_outputs(clean_df, issues_df, name):
    clean_df.to_csv(OUT_DIR / f"{name}.csv", index=False)
    issues_df.fillna("").to_csv(OUT_DIR / f"{name}_issues.csv", index=False)

    print(f"[OK] {name}: Clean={len(clean_df):,}, Issues={len(issues_df):,}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n=== Cleaning Marketing Campaign Data (Type 1) ===\n")

    campaign_df = load_and_clean_campaign(CAMPAIGN_FILE)
    campaign_clean, campaign_issues = split_clean_and_issues(campaign_df)

    save_outputs(campaign_clean, campaign_issues, "campaign_data")

    print("Campaign cleaning completed ✓")

if __name__ == "__main__":
    main()
