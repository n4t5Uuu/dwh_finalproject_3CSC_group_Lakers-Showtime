# ======================================================
# CUSTOMER MANAGEMENT CLEANING (COMBINED USER DATA)
# ======================================================
# Rules:
# - Files are row-aligned (historical snapshot)
# - Dataset is frozen
# - user_data.name is authoritative
# - Name mismatches across tables are data-quality issues
# - No deduplication or SCD logic here
# ======================================================

import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #

RAW_DIR = Path("/data_files/Customer Management Department")
OUT_DIR = Path("/clean_data/customer_management")
OUT_DIR.mkdir(parents=True, exist_ok=True)

USER_DATA_FILE = RAW_DIR / "user_data.csv"
USER_JOB_FILE  = RAW_DIR / "user_job.csv"
USER_CC_FILE   = RAW_DIR / "user_credit_card.csv"

OUT_FILE = OUT_DIR / "user_data_all.csv"
ISSUES_FILE = OUT_DIR / "user_data_all_issues.csv"

# ======================================================
# MAIN CLEANING LOGIC
# ======================================================

def main():
    print("=== Cleaning Customer Management (Combined User Data) ===\n")

    # --------------------------------------------------
    # 1. Load raw files
    # --------------------------------------------------
    user_df = pd.read_csv(USER_DATA_FILE)
    job_df  = pd.read_csv(USER_JOB_FILE)
    cc_df   = pd.read_csv(USER_CC_FILE)

    # --------------------------------------------------
    # 2. Explicit row-alignment key
    # --------------------------------------------------
    user_df["_row_id"] = user_df.index
    job_df["_row_id"]  = job_df.index
    cc_df["_row_id"]   = cc_df.index

    # --------------------------------------------------
    # 3. Row-by-row merge
    # --------------------------------------------------
    combined = (
        user_df
        .merge(job_df, on="_row_id", how="left", suffixes=("", "_job"))
        .merge(cc_df,  on="_row_id", how="left", suffixes=("", "_cc"))
        .drop(columns=["_row_id"], errors="ignore")
    )

    # --------------------------------------------------
    # 4. Drop technical & duplicate identifier columns
    # --------------------------------------------------
    combined = combined.drop(
        columns=[
            "Unnamed: 0",
            "user_id_job",
            "user_id_cc",
        ],
        errors="ignore"
    )

    # --------------------------------------------------
    # 5. Name consistency check
    # --------------------------------------------------
    # user_data.name is authoritative
    name_main = combined["name"].astype(str).str.strip()

    name_job = (
        combined.get("name_job")
        .astype(str)
        .str.strip()
        if "name_job" in combined.columns
        else pd.Series("", index=combined.index)
    )

    name_cc = (
        combined.get("name_cc")
        .astype(str)
        .str.strip()
        if "name_cc" in combined.columns
        else pd.Series("", index=combined.index)
    )

    name_mismatch = (
        ((name_job != "") & (name_job != name_main)) |
        ((name_cc != "") & (name_cc != name_main))
    )

    # --------------------------------------------------
    # 6. Drop redundant name columns AFTER check
    # --------------------------------------------------
    combined = combined.drop(columns=["name_job", "name_cc"], errors="ignore")

    # --------------------------------------------------
    # 7. Type enforcement
    # --------------------------------------------------
    str_cols = [
        "user_id", "name", "street", "state", "city",
        "country", "gender", "device_address", "user_type",
        "job_title", "job_level",
        "credit_card_number", "issuing_bank"
    ]

    for col in str_cols:
        if col in combined.columns:
            combined[col] = combined[col].astype(str)

    # Dates
    combined["creation_date"] = pd.to_datetime(
        combined["creation_date"], errors="coerce"
    )
    combined["birthdate"] = pd.to_datetime(
        combined["birthdate"], errors="coerce"
    )

    # --------------------------------------------------
    # 8. Business rule normalization
    # --------------------------------------------------
    combined["country"] = "United States"

    # Normalize Student job records
    is_student = combined["job_title"].str.lower() == "student"
    job_level_null = combined["job_level"].isna() | (combined["job_level"] == "nan")

    combined.loc[is_student & job_level_null, "job_level"] = "Student"

    # --------------------------------------------------
    # 9. Required-field validation
    # --------------------------------------------------
    REQUIRED_COLS = ["user_id", "name", "creation_date"]
    required_ok = combined[REQUIRED_COLS].notna().all(axis=1)

    # --------------------------------------------------
    # 10. Final clean vs issues split
    # --------------------------------------------------
    clean_mask = required_ok & ~name_mismatch

    clean = combined.loc[clean_mask].copy()
    issues = combined.loc[~clean_mask].copy()

    # --------------------------------------------------
    # 11. Save outputs
    # --------------------------------------------------
    clean.to_csv(OUT_FILE, index=False)

    if not issues.empty:
        issues.fillna("").to_csv(ISSUES_FILE, index=False)

    print(f"Clean rows  : {len(clean):,}")
    print(f"Issue rows  : {len(issues):,}")
    print(f"Output file: {OUT_FILE}")

    print("\nCustomer Management cleaning complete ✅")


if __name__ == "__main__":
    main()
