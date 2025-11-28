import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #
RAW_DIR = Path("/data_files") / "Customer Management Department"  # folder where your CSVs are located

USER_DATA_FILE = RAW_DIR / "user_data.csv"
USER_JOB_FILE = RAW_DIR / "user_job.csv"
USER_CC_FILE = RAW_DIR / "user_credit_card.csv"

OUT_DIR = Path("/clean_data") / "customer_management"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


def load_user_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_data.csv"""
    df = pd.read_csv(path)

    # Enforce types
    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["gender"] = df["gender"].astype(str)
    df["device_address"] = df["device_address"].astype(str)
    df["user_type"] = df["user_type"].astype(str)

    # Parse datetimes (invalid → NaT, treated as issue later)
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    return df


def load_user_job(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_job.csv"""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["job_title"] = df["job_title"].astype(str)
    # job_level can be NaN, keep nullable/string
    df["job_level"] = df["job_level"].astype("string")

    return df


def load_user_credit_card(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_credit_card.csv"""
    df = pd.read_csv(path)

    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    # credit_card_number must be string (ID, not number)
    df["credit_card_number"] = df["credit_card_number"].astype(str)
    df["issuing_bank"] = df["issuing_bank"].astype(str)

    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Generic splitter for datasets where we don't need special rules.

    - Duplicates based on key_cols (or entire row if key_cols is None)
    - Any row with at least one null/NaT is an issue
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
      - clean CSV + Parquet with base name
      - issues CSV as {name}_issues.csv
    """
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    # Clean data
    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    # Issues: replace NaN/NaT with empty string for easier manual review
    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows:  {len(clean_df):,}")
    print(f"  Issue rows:  {len(issues_df):,}")
    print(f"  CSV:         {csv_path}")
    print(f"  Parquet:     {parquet_path}")
    print(f"  Issues CSV:  {issues_path}\n")


def main():
    print("=== Ingesting Customer Management datasets ===\n")

    # ---------- user_data ---------- #
    print("Loading user_data.csv ...")
    user_data_raw = load_user_data(USER_DATA_FILE)
    print(user_data_raw.head(), "\n")
    print(user_data_raw.dtypes, "\n")

    # Duplicates based on user_id
    user_data_clean, user_data_issues = split_clean_and_issues(
        user_data_raw, key_cols=["user_id"]
    )
    save_outputs(user_data_clean, user_data_issues, "user_data")

    # ---------- user_job ---------- #
    print("Loading user_job.csv ...")
    user_job_raw = load_user_job(USER_JOB_FILE)
    print(user_job_raw.head(), "\n")
    print(user_job_raw.dtypes, "\n")

    # Duplicates based on (user_id, job_title)
    duplicate_mask = user_job_raw.duplicated(
        subset=["user_id", "job_title"],
        keep=False,
    )

    # Base null mask (any null in any column)
    null_mask_all = user_job_raw.isna().any(axis=1)

    # Allowed case: job_title == "Student" AND job_level is null
    job_title_lower = user_job_raw["job_title"].str.lower()
    allowed_student_null_level = job_title_lower.eq("student") & user_job_raw[
        "job_level"
    ].isna()

    # Final null mask: nulls that are NOT the allowed Student-null-job_level case
    null_mask = null_mask_all & ~allowed_student_null_level

    # Anything duplicate OR with disallowed nulls is an issue
    bad_mask = duplicate_mask | null_mask

    user_job_issues = user_job_raw.loc[bad_mask].copy()
    user_job_clean = user_job_raw.loc[~bad_mask].copy()

    save_outputs(user_job_clean, user_job_issues, "user_job")

    # ---------- user_credit_card ---------- #
    print("Loading user_credit_card.csv ...")
    user_cc_raw = load_user_credit_card(USER_CC_FILE)
    print(user_cc_raw.head(), "\n")
    print(user_cc_raw.dtypes, "\n")

    # Duplicates based on (user_id, credit_card_number)
    user_cc_clean, user_cc_issues = split_clean_and_issues(
        user_cc_raw, key_cols=["user_id", "credit_card_number"]
    )
    save_outputs(user_cc_clean, user_cc_issues, "user_credit_card")

    print("Customer Management ingestion complete ✅")


if __name__ == "__main__":
    main()