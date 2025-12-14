# Cleaning Script for Enterprise Department Tables (FINAL FIXED VERSION)

from pathlib import Path
import pandas as pd
import re

SCRIPT_DIR = Path(__file__).resolve().parent          # ...\scripts\enterprise_scripts
PROJECT_ROOT = SCRIPT_DIR.parents[1]                  # ...\dwh_finalproject_3CSC_group_Lakers-Showtime

RAW_DIR = PROJECT_ROOT / "data_files" / "Enterprise Department"
OUT_DIR = PROJECT_ROOT / "clean_data" / "enterprise"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MERCHANT_FILE = RAW_DIR / "merchant_data.csv"
STAFF_FILE    = RAW_DIR / "staff_data.csv"
ORDER_FILES = [
    RAW_DIR / "order_with_merchant_data1.csv",
    RAW_DIR / "order_with_merchant_data2.csv",
    RAW_DIR / "order_with_merchant_data3.csv",
]
# ============================================================
# HELPERS
# ============================================================

def to_date_key(dt: pd.Series) -> pd.Series:
    """
    Convert datetime series to int YYYYMMDD.
    NaT -> <NA>.
    """
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def normalize_phone(phone: str) -> str:
    """Normalize phone numbers to consistent format."""
    if pd.isna(phone):
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return digits


def clean_merchant_name(name: str) -> str:
    """Remove extra quotes and spacing."""
    if pd.isna(name):
        return ""
    cleaned = str(name).strip().strip('"').strip("'")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def normalize_business_id(raw_id: str, prefix: str) -> str:
    """
    Convert:
    MERCHANT4459 → MERCHANT04459
    STAFF3321 → STAFF03321
    """
    if pd.isna(raw_id):
        return ""

    s = str(raw_id).strip()
    m = re.match(rf"^{prefix}(\d+)$", s)
    if not m:
        return s

    num = m.group(1).zfill(5)
    return f"{prefix}{num}"


def assign_surrogate_keys(df: pd.DataFrame, id_col: str, key_col: str):
    """
    For duplicates:
    MERCHANT04459 → MERCHANT04459-1, MERCHANT04459-2
    """
    df = df.copy()
    df["_orig_index"] = df.index
    df[id_col] = df[id_col].astype(str)

    def assign_group(group):
        group = group.sort_values("_orig_index")
        base = group[id_col].iloc[0]
        group["seq"] = range(1, len(group) + 1)
        group[key_col] = [f"{base}-{i}" for i in group["seq"]]
        return group

    df = df.groupby(id_col, group_keys=False).apply(assign_group)
    df = df.sort_values("_orig_index").drop(columns=["_orig_index", "seq"])
    return df


def split_clean_and_issues(df: pd.DataFrame, key_cols=None):
    if key_cols:
        dup_mask = df.duplicated(subset=key_cols, keep=False)
    else:
        dup_mask = df.duplicated(keep=False)

    null_mask = df.isna().any(axis=1)
    issue_mask = dup_mask | null_mask

    return df[~issue_mask].copy(), df[issue_mask].copy()


def save_outputs(clean_df, issues_df, name):
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"
    issues_path = OUT_DIR / f"{name}_issues.csv"

    clean_df.to_csv(csv_path, index=False)
    clean_df.to_parquet(parquet_path, index=False)
    issues_df.fillna("").to_csv(issues_path, index=False)

    print(f"[OK] Saved {name} → Clean: {len(clean_df):,}, Issues: {len(issues_df):,}")


# ============================================================
# LOADERS
# ============================================================

def load_merchant_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["merchant_id"] = df["merchant_id"].astype(str)
    df["merchant_id"] = df["merchant_id"].apply(lambda x: normalize_business_id(x, "MERCHANT"))
    df["name"] = df["name"].astype(str).apply(clean_merchant_name)

    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).apply(normalize_phone)

    # ===================== NEW DATE PARSING =====================
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
        df["merchant_creation_date_key"] = to_date_key(df["creation_date"])
    else:
        df["merchant_creation_date_key"] = pd.NA
    # ============================================================

    df["country"] = "United States"
    return df

def load_staff_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["staff_id"] = df["staff_id"].astype(str)
    df["staff_id"] = df["staff_id"].apply(lambda x: normalize_business_id(x, "STAFF"))

    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).apply(normalize_phone)

    # ===================== NEW DATE PARSING =====================
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
        df["staff_creation_date_key"] = to_date_key(df["creation_date"])
    else:
        df["staff_creation_date_key"] = pd.NA
    # ============================================================

    df["country"] = "United States"
    return df



def load_order_files(paths):
    frames = []
    for p in paths:
        df = pd.read_csv(p)
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


# ============================================================
# ATTACHING KEYS
# ============================================================

def attach_keys_to_orders(order_df, merchant_df, staff_df):
    df = order_df.copy()

    # MERCHANT ID FIX (pad raw merchant_id too)
    if "merchant_id" in df.columns:
        df["merchant_id"] = df["merchant_id"].astype(str).apply(lambda x: normalize_business_id(x, "MERCHANT"))

    if "staff_id" in df.columns:
        df["staff_id"] = df["staff_id"].astype(str).apply(lambda x: normalize_business_id(x, "STAFF"))

    # ===================== NEW DATE PARSING =====================
    
    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        df["transaction_date_key"] = to_date_key(df["transaction_date"])
    else:
        df["transaction_date_key"] = pd.NA

    # ============================================================


    # MERCHANT JOIN
    merchant_map = merchant_df[["merchant_id", "merchant_key"]].drop_duplicates()
    df = df.merge(merchant_map, on="merchant_id", how="left")

    # STAFF JOIN
    staff_map = staff_df[["staff_id", "staff_key"]].drop_duplicates()
    df = df.merge(staff_map, on="staff_id", how="left")

    # DROP RAW IDs
    df = df.drop(columns=["merchant_id", "staff_id"], errors="ignore")

    # KEEP ONLY ONE ROW PER ORDER
    df = df.sort_values("order_id").drop_duplicates(subset=["order_id"], keep="first")

    return df


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    print("\n=== Cleaning Enterprise Department ===\n")

    # 1. MERCHANT DATA
    merchant_raw = load_merchant_data(MERCHANT_FILE)
    merchant_with_keys = assign_surrogate_keys(merchant_raw, "merchant_id", "merchant_key")
    merchant_clean, merchant_issues = split_clean_and_issues(merchant_with_keys, key_cols=["merchant_key"])
    save_outputs(merchant_clean, merchant_issues, "merchant_data")

    # 2. STAFF DATA
    staff_raw = load_staff_data(STAFF_FILE)
    staff_with_keys = assign_surrogate_keys(staff_raw, "staff_id", "staff_key")
    staff_clean, staff_issues = split_clean_and_issues(staff_with_keys, key_cols=["staff_key"])
    save_outputs(staff_clean, staff_issues, "staff_data")

    # 3. ORDER → MERCHANT + STAFF
    orders_raw = load_order_files(ORDER_FILES)
    orders_with_keys = attach_keys_to_orders(orders_raw, merchant_with_keys, staff_with_keys)

    order_clean, order_issues = split_clean_and_issues(orders_with_keys, key_cols=["order_id"])
    save_outputs(order_clean, order_issues, "order_with_merchant_data_all")

    print("Enterprise cleaning completed successfully ✓")


if __name__ == "__main__":
    main()