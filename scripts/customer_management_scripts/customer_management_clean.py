# Cleaning Script for Customer Management Department Tables

import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #
# Get Root:
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
RAW_DIR = PROJECT_ROOT / "data_files" / "Customer Management Department"

# Raw/Input Files:
USER_DATA_FILE = RAW_DIR / "user_data.csv"
USER_JOB_FILE = RAW_DIR / "user_job.csv"
USER_CC_FILE = RAW_DIR / "user_credit_card.csv"

# Outputs
OUT_DIR = Path("clean_data") / "customer_management"
OUT_DIR.mkdir(parents=True, exist_ok=True)
# ================== CONFIG ================== #


# ---------- Helpers shared by all parts ---------- #

def _digits_from_user_id(uid: str) -> str:
    """Extract numeric part from something like 'USER012195'."""
    return "".join(ch for ch in str(uid) if ch.isdigit())


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    """
    Generic splitter.

    - Issues = duplicates based on key_cols (or full row if None)
              OR any null/NaT in any column.
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
    """Save clean and issues files (CSV + Parquet)."""
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    # Clean data
    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    # Issues: blanks instead of NaN for manual review
    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows:  {len(clean_df):,}")
    print(f"  Issue rows:  {len(issues_df):,}")
    print(f"  CSV:         {csv_path}")
    print(f"  Parquet:     {parquet_path}")
    print(f"  Issues CSV:  {issues_path}\n")


# ---------- USER DATA ---------- #

def load_user_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean user_data.csv."""
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

    # Force all countries to United States (per your latest version)
    df["country"] = "United States"

    return df


def assign_user_ids_and_keys(user_data: pd.DataFrame):
    """
    Renumber duplicate user_ids and create user_key, while
    preserving the original row order of user_data.
    """

    df = user_data.copy()

    # Keep original user_id for mapping + original row order
    df["user_id_orig"] = df["user_id"].astype(str)
    df["_orig_index"] = df.index  # remember original order

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        # keep original order within the group
        group = group.sort_values("_orig_index").copy()

        base_uid = group["user_id_orig"].iloc[0]
        digits = _digits_from_user_id(base_uid)

        group["seq"] = range(1, len(group) + 1)
        n = len(group)

        if n > 1:
            # Renumber user_id for duplicates
            group["user_id"] = [f"USER{digits}{i}" for i in group["seq"]]
        else:
            # Keep original user_id for non-dupe
            group["user_id"] = base_uid

        # user_key always uses base digits and seq
        group["user_key"] = [f"{digits}-{i}" for i in group["seq"]]

        return group

    # groupby with sort=False so groups follow first appearance in the file
    df = df.groupby("user_id_orig", sort=False,
                    group_keys=False).apply(process_group)

    # Build mapping from original IDs to user_key
    mapping = df[["user_id_orig", "name", "user_key"]].drop_duplicates()

    # Restore original row order and drop helper columns
    df = df.sort_values("_orig_index").drop(columns=["seq", "_orig_index"])

    return df, mapping

# ---------- USER JOB (combined cleaning + ingest) ---------- #


def load_and_clean_user_job(path: Path, mapping: pd.DataFrame) -> pd.DataFrame:
    """Load user_job.csv, apply Student fix, attach user_key from mapping."""
    df = pd.read_csv(path)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize strings
    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["job_title"] = df["job_title"].astype(str).str.strip()

    # job_level may not exist for some rows; ensure column
    if "job_level" not in df.columns:
        df["job_level"] = pd.NA

    # Fix Student rows with null job_level (logic from user_job_clean.py)
    is_student = df["job_title"].str.lower() == "student"
    is_level_null = df["job_level"].isna()
    fix_mask = is_student & is_level_null
    df.loc[fix_mask, "job_level"] = "Student"

    # Attach user_key based on mapping from user_data
    df = df.merge(
        mapping,
        left_on=["user_id", "name"],
        right_on=["user_id_orig", "name"],
        how="left",
    )

    # Drop helper ID columns (no more original user_id in final output)
    drop_cols = [c for c in ["user_id", "user_id_orig"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Reorder columns: user_key first if it exists
    if "user_key" in df.columns:
        cols = ["user_key"] + [c for c in df.columns if c != "user_key"]
        df = df[cols]

    # Ensure job_level is nullable string
    df["job_level"] = df["job_level"].astype("string")

    return df


# ---------- USER CREDIT CARD (combined cleaning + ingest) ---------- #

def load_and_clean_user_credit_card(path: Path, mapping: pd.DataFrame) -> pd.DataFrame:
    """Load user_credit_card.csv, attach user_key from mapping."""
    df = pd.read_csv(path)

    # Drop auto-generated index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Normalize columns
    if "user_id" in df.columns:
        df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["credit_card_number"] = df["credit_card_number"].astype(str)
    df["issuing_bank"] = df["issuing_bank"].astype(str)

    # Attach user_key based on mapping from user_data
    df = df.merge(
        mapping,
        left_on=["user_id", "name"],
        right_on=["user_id_orig", "name"],
        how="left",
    )

    # Drop helper ID columns (no more original user_id in final output)
    drop_cols = [c for c in ["user_id", "user_id_orig"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Reorder columns: user_key first if present
    if "user_key" in df.columns:
        cols = ["user_key"] + [c for c in df.columns if c != "user_key"]
        df = df[cols]

    return df


# ---------- MAIN PIPELINE ---------- #

def main():
    print("=== Ingesting Customer Management datasets (combined) ===\n")

    # ---------- user_data ---------- #
    print("Loading user_data.csv ...")
    user_data_raw = load_user_data(USER_DATA_FILE)
    print(user_data_raw.head(), "\n")
    print(user_data_raw.dtypes, "\n")

    # Renumber duplicate user_ids and create user_key + mapping
    user_data_fixed, user_key_mapping = assign_user_ids_and_keys(user_data_raw)

    # user_data: after renumbering, user_id should be unique; issues mostly null rows
    user_data_clean, user_data_issues = split_clean_and_issues(
        user_data_fixed, key_cols=["user_id"]
    )
    save_outputs(user_data_clean, user_data_issues, "user_data")

    # ---------- user_job (clean + ingest) ---------- #
    print("Loading user_job.csv ...")
    user_job_with_keys = load_and_clean_user_job(
        USER_JOB_FILE, user_key_mapping)
    print(user_job_with_keys.head(), "\n")
    print(user_job_with_keys.dtypes, "\n")

    # Duplicates based on (user_key, job_title)
    user_job_clean, user_job_issues = split_clean_and_issues(
        user_job_with_keys, key_cols=["user_key", "job_title"]
    )
    save_outputs(user_job_clean, user_job_issues, "user_job")

    # ---------- user_credit_card (clean + ingest) ---------- #
    print("Loading user_credit_card.csv ...")
    user_cc_with_keys = load_and_clean_user_credit_card(
        USER_CC_FILE, user_key_mapping)
    print(user_cc_with_keys.head(), "\n")
    print(user_cc_with_keys.dtypes, "\n")

    # Duplicates based on (user_key, credit_card_number)
    user_cc_clean, user_cc_issues = split_clean_and_issues(
        user_cc_with_keys, key_cols=["user_key", "credit_card_number"]
    )
    save_outputs(user_cc_clean, user_cc_issues, "user_credit_card")

    print("Customer Management ingestion complete ✅")


if __name__ == "__main__":
    main()
