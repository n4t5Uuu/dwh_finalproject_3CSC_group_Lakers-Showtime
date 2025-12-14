import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

RAW_CUSTOMER_DIR = PROJECT_ROOT / "data_files" / "Customer Management Department"
RAW_OPERATIONS_DIR = PROJECT_ROOT / "data_files" / "Operations Department"

USER_DATA_FILE = RAW_CUSTOMER_DIR / "user_data.csv"
USER_JOB_FILE = RAW_CUSTOMER_DIR / "user_job.csv"
USER_CC_FILE = RAW_CUSTOMER_DIR / "user_credit_card.csv"

ORDER_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20211001.csv",
    "order_data_20211001-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
]

OUT_DIR = PROJECT_ROOT / "clean_data" / "customer_management"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ================== HELPERS ================== #

def _digits_from_user_id(uid: str) -> str:
    return "".join(ch for ch in str(uid) if ch.isdigit())

def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
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
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)

    issues_df = issues_df.copy()
    issues_df.fillna("", inplace=True)
    issues_df.to_csv(issues_path, index=False)

    print(f"Saved {name}:")
    print(f"  Clean rows: {len(clean_df):,}")
    print(f"  Issue rows: {len(issues_df):,}")
    print(f"  CSV: {csv_path}")
    print(f"  Parquet: {parquet_path}")
    print(f"  Issues CSV: {issues_path}\n")

# ================== LOAD FUNCTIONS ================== #

def load_user_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["gender"] = df["gender"].astype(str)
    df["device_address"] = df["device_address"].astype(str)
    df["user_type"] = df["user_type"].astype(str)

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    df["country"] = "United States"
    return df

def load_all_orders() -> pd.DataFrame:
    dfs = []
    for fname in ORDER_FILES:
        fpath = RAW_OPERATIONS_DIR / fname
        if not fpath.exists():
            print(f"[WARN] Missing order file (skipped): {fpath}")
            continue

        df = pd.read_csv(fpath)
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        if "transaction_date" in df.columns:
            df["transaction_date"] = pd.to_datetime(
                df["transaction_date"], errors="coerce"
            )
        dfs.append(df)

    if not dfs:
        print("[WARN] No orders loaded for creation-date alignment.")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

# ================== USER CREATION EFFECTIVE DATE ================== #

def add_user_creation_effective_date(user_df: pd.DataFrame,
                                     orders_df: pd.DataFrame) -> pd.DataFrame:
    df = user_df.copy()
    df["user_creation_effective_date"] = df["creation_date"]

    if orders_df.empty or "transaction_date" not in orders_df.columns:
        print("[INFO] No valid orders to adjust creation dates; using creation_date as effective date.")
        return df

    orders_df = orders_df.copy()
    orders_df["user_id"] = orders_df["user_id"].astype(str)
    orders_df["transaction_date"] = pd.to_datetime(
        orders_df["transaction_date"], errors="coerce"
    )

    earliest_tx = (
        orders_df
        .dropna(subset=["transaction_date"])
        .groupby("user_id", as_index=False)["transaction_date"]
        .min()
        .rename(columns={"transaction_date": "earliest_transaction_date"})
    )

    df["_orig_index"] = df.index
    df = df.merge(earliest_tx, on="user_id", how="left")

    df = df.sort_values("_orig_index")
    df["is_first_for_user"] = ~df.duplicated(subset=["user_id"])

    cond = (
        df["is_first_for_user"]
        & df["earliest_transaction_date"].notna()
        & (
            df["creation_date"].isna()
            | (df["earliest_transaction_date"] < df["creation_date"])
        )
    )

    df.loc[cond, "user_creation_effective_date"] = df.loc[
        cond, "earliest_transaction_date"
    ]

    df = df.drop(columns=[
        "earliest_transaction_date",
        "is_first_for_user",
        "_orig_index",
    ])

    return df

def build_canonical_user_map(user_df: pd.DataFrame) -> pd.DataFrame:
    df = user_df.copy()
    df["user_creation_effective_date"] = pd.to_datetime(
        df["user_creation_effective_date"], errors="coerce"
    )

    if "user_id_orig" not in df.columns:
        df["user_id_orig"] = df["user_id"].astype(str)

    canonical = (
        df.sort_values(["user_id_orig", "user_creation_effective_date"])
          .groupby("user_id_orig", as_index=False)
          .head(1)
    )

    return canonical[["user_id_orig", "user_key"]].rename(
        columns={"user_key": "canonical_user_key"}
    )

# ================== USER ID / KEY ASSIGNMENT ================== #

def assign_user_ids_and_keys(user_data: pd.DataFrame):
    df = user_data.copy()

    df["user_id_orig"] = df["user_id"].astype(str)
    df["_orig_index"] = df.index

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("_orig_index").copy()

        base_uid = group["user_id_orig"].iloc[0]
        digits = _digits_from_user_id(base_uid)
        group["seq"] = range(1, len(group) + 1)

        if len(group) > 1:
            group["user_id"] = [f"USER{digits}{i}" for i in group["seq"]]
        else:
            group["user_id"] = base_uid

        if "user_type" in group.columns:
            user_type_first = str(group["user_type"].iloc[0]).lower()
        else:
            user_type_first = "user"

        if user_type_first in ("staff", "employee"):
            prefix = "S"
        elif user_type_first == "merchant":
            prefix = "M"
        else:
            prefix = "U"

        group["user_key"] = [f"{prefix}{digits}-{i}" for i in group["seq"]]
        return group

    df = df.groupby("user_id_orig", sort=False,
                    group_keys=False).apply(process_group)

    mapping = df[["user_id_orig", "name", "user_key"]].drop_duplicates()
    df = df.sort_values("_orig_index").drop(columns=["seq", "_orig_index"])
    return df, mapping

# ================== USER JOB ================== #

def load_and_clean_user_job(path: Path, mapping: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["job_title"] = df["job_title"].astype(str).str.strip()

    if "job_level" not in df.columns:
        df["job_level"] = pd.NA

    is_student = df["job_title"].str.lower() == "student"
    is_level_null = df["job_level"].isna()
    fix_mask = is_student & is_level_null
    df.loc[fix_mask, "job_level"] = "Student"

    df = df.merge(
        mapping,
        left_on=["user_id", "name"],
        right_on=["user_id_orig", "name"],
        how="left",
    )

    drop_cols = [c for c in ["user_id", "user_id_orig"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    if "user_key" in df.columns:
        cols = ["user_key"] + [c for c in df.columns if c != "user_key"]
        df = df[cols]

    df["job_level"] = df["job_level"].astype("string")
    return df

# ================== USER CREDIT CARD ================== #

def load_and_clean_user_credit_card(path: Path, mapping: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    if "user_id" in df.columns:
        df["user_id"] = df["user_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["credit_card_number"] = df["credit_card_number"].astype(str)
    df["issuing_bank"] = df["issuing_bank"].astype(str)

    df = df.merge(
        mapping,
        left_on=["user_id", "name"],
        right_on=["user_id_orig", "name"],
        how="left",
    )

    drop_cols = [c for c in ["user_id", "user_id_orig"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    if "user_key" in df.columns:
        cols = ["user_key"] + [c for c in df.columns if c != "user_key"]
        df = df[cols]

    return df

# ================== MAIN PIPELINE ================== #

def main():
    print("=== Ingesting Customer Management datasets (combined) ===\n")

    print("Loading user_data.csv ...")
    user_data_raw = load_user_data(USER_DATA_FILE)
    print(user_data_raw.head(), "\n")
    print(user_data_raw.dtypes, "\n")

    print("Loading orders for user_creation_effective_date ...")
    all_orders = load_all_orders()

    print("Calculating user_creation_effective_date per user_id ...")
    user_data_with_effective = add_user_creation_effective_date(
        user_data_raw, all_orders
    )

    user_data_fixed, _basic_mapping = assign_user_ids_and_keys(
        user_data_with_effective
    )

    canonical_user_map = build_canonical_user_map(user_data_fixed)

    user_key_mapping = (
        user_data_fixed[["user_id_orig", "name", "user_key"]]
        .drop_duplicates()
        .merge(canonical_user_map, on="user_id_orig", how="left")
    )

    canonical_path = OUT_DIR / "user_canonical_keys.csv"
    canonical_user_map.to_csv(canonical_path, index=False)
    print(f"Saved canonical user map -> {canonical_path}")

    user_data_clean, user_data_issues = split_clean_and_issues(
        user_data_fixed, key_cols=["user_id"]
    )
    save_outputs(user_data_clean, user_data_issues, "user_data")

    print("Loading user_job.csv ...")
    user_job_with_keys = load_and_clean_user_job(
        USER_JOB_FILE, user_key_mapping
    )
    print(user_job_with_keys.head(), "\n")
    print(user_job_with_keys.dtypes, "\n")

    user_job_clean, user_job_issues = split_clean_and_issues(
        user_job_with_keys, key_cols=["user_key", "job_title"]
    )
    save_outputs(user_job_clean, user_job_issues, "user_job")

    print("Loading user_credit_card.csv ...")
    user_cc_with_keys = load_and_clean_user_credit_card(
        USER_CC_FILE, user_key_mapping
    )
    print(user_cc_with_keys.head(), "\n")
    print(user_cc_with_keys.dtypes, "\n")

    user_cc_clean, user_cc_issues = split_clean_and_issues(
        user_cc_with_keys, key_cols=["user_key", "credit_card_number"]
    )
    save_outputs(user_cc_clean, user_cc_issues, "user_credit_card")

    print("Customer Management ingestion complete")

if __name__ == "__main__":
    main()
