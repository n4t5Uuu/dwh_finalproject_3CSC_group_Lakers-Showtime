import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #
# This file lives in: scripts/customer_management_scripts/
SCRIPT_DIR = Path(__file__).resolve().parent
# .../dwh_finalproject_3CSC_group_Lakers-Showtime
PROJECT_ROOT = SCRIPT_DIR.parents[1]

# RAW user_data (still in data_files)
RAW_CM_DIR = PROJECT_ROOT / "data_files" / "Customer Management Department"

# CLEANED user_job & user_credit_card (output of your earlier cleaners)
CLEAN_CM_DIR = PROJECT_ROOT / "clean_data" / "customer_management"

USER_DATA_FILE = RAW_CM_DIR / "user_data.csv"
USER_JOB_FILE = CLEAN_CM_DIR / "user_job_clean.csv"
USER_CC_FILE = CLEAN_CM_DIR / "user_credit_card_clean.csv"

OUT_DIR = PROJECT_ROOT / "clean_data" / "customer_management"
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

    # Parse datetimes (invalid -> NaT)
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    # Force all countries to United States
    df["country"] = "United States"

    return df


def load_user_job(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_job_clean.csv (uses user_key, not user_id)."""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    if "user_key" in df.columns:
        df["user_key"] = df["user_key"].astype(str)

    if "name" in df.columns:
        df["name"] = df["name"].astype(str)

    if "job_title" in df.columns:
        df["job_title"] = df["job_title"].astype(str)

    if "job_level" in df.columns:
        # job_level can be NaN, keep nullable/string
        df["job_level"] = df["job_level"].astype("string")

    return df


def load_user_credit_card(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_credit_card_clean.csv (uses user_key, not user_id)."""
    df = pd.read_csv(path)

    # Drop useless index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # credit-card file now has user_key instead of user_id
    if "user_key" in df.columns:
        df["user_key"] = df["user_key"].astype(str)

    if "name" in df.columns:
        df["name"] = df["name"].astype(str)

    if "credit_card_number" in df.columns:
        # treat as string ID, not numeric
        df["credit_card_number"] = df["credit_card_number"].astype(str)

    if "issuing_bank" in df.columns:
        df["issuing_bank"] = df["issuing_bank"].astype(str)

    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """Generic splitter.

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


def _digits_from_user_id(uid: str) -> str:
    """Extract numeric part from something like 'USER40678'."""
    return "".join(ch for ch in uid if ch.isdigit())


def renumber_user_id_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Renumber duplicate user_ids and create user_key.

    For each original user_id group:
      - Sort by creation_date (earliest first, NaT last) just inside the group.
      - Assign seq = 1..N.

    If N > 1 (dupes):
        USER40678 (seq 1) -> user_id = USER406781, user_key = 40678-1
        USER40678 (seq 2) -> user_id = USER406782, user_key = 40678-2

    If N == 1 (non-dupe):
        USER61846       -> user_id = USER61846,  user_key = 61846-1

    Final DataFrame is put back into the ORIGINAL row order.
    """
    df = df.copy()
    # remember original order
    df["orig_idx"] = df.index

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        # sort only within this user_id group to find earliest creation
        group = group.sort_values("creation_date", na_position="last").copy()
        base_uid = group["user_id"].iloc[0]
        digits = _digits_from_user_id(base_uid)

        group["seq"] = range(1, len(group) + 1)
        n = len(group)

        if n > 1:
            # Renumber user_id for dupes
            group["user_id"] = [f"USER{digits}{i}" for i in group["seq"]]
        else:
            # Keep original user_id for non-dupe
            group["user_id"] = base_uid

        # user_key always: <base_digits>-<seq>
        group["user_key"] = [f"{digits}-{i}" for i in group["seq"]]

        return group

    df = df.groupby("user_id", group_keys=False).apply(process_group)

    # drop seq helper
    df = df.drop(columns=["seq"])

    # restore original row order
    df = df.sort_values("orig_idx").drop(columns=["orig_idx"])

    return df


def save_outputs(clean_df: pd.DataFrame, issues_df: pd.DataFrame, name: str):
    """Save clean and issues files (CSV + Parquet)."""
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

    # Renumber duplicate user_ids and add user_key
    user_data_fixed = renumber_user_id_duplicates(user_data_raw)

    # After renumbering, user_id should be unique; issues are mostly null/NaT rows
    user_data_clean, user_data_issues = split_clean_and_issues(
        user_data_fixed, key_cols=["user_id"]
    )
    save_outputs(user_data_clean, user_data_issues, "user_data")

    # ---------- user_job (already cleaned to use user_key) ---------- #
    print("Loading user_job_clean.csv ...")
    user_job_raw = load_user_job(USER_JOB_FILE)
    print(user_job_raw.head(), "\n")
    print(user_job_raw.dtypes, "\n")

    # Duplicates based on (user_key, job_title)
    duplicate_mask = user_job_raw.duplicated(
        subset=["user_key", "job_title"],
        keep=False,
    )

    # Base null mask (any null in any column)
    null_mask_all = user_job_raw.isna().any(axis=1)

    # Allowed case: job_title == "Student" AND job_level is null
    if "job_title" in user_job_raw.columns and "job_level" in user_job_raw.columns:
        job_title_lower = user_job_raw["job_title"].str.lower()
        allowed_student_null_level = job_title_lower.eq("student") & user_job_raw[
            "job_level"
        ].isna()
    else:
        allowed_student_null_level = pd.Series(False, index=user_job_raw.index)

    # Final null mask: nulls that are NOT the allowed Student-null-job_level case
    null_mask = null_mask_all & ~allowed_student_null_level

    # Anything duplicate OR with disallowed nulls is an issue
    bad_mask = duplicate_mask | null_mask

    user_job_issues = user_job_raw.loc[bad_mask].copy()
    user_job_clean = user_job_raw.loc[~bad_mask].copy()

    save_outputs(user_job_clean, user_job_issues, "user_job")

    # ---------- user_credit_card (now using user_key) ---------- #
    print("Loading user_credit_card_clean.csv ...")
    user_cc_raw = load_user_credit_card(USER_CC_FILE)
    print(user_cc_raw.head(), "\n")
    print(user_cc_raw.dtypes, "\n")

    # Duplicates based on (user_key, credit_card_number)
    user_cc_clean, user_cc_issues = split_clean_and_issues(
        user_cc_raw, key_cols=["user_key", "credit_card_number"]
    )
    save_outputs(user_cc_clean, user_cc_issues, "user_credit_card")

    print("Customer Management ingestion complete ✅")


if __name__ == "__main__":
    main()
