from pathlib import Path
import pandas as pd
import re

# ===== CONFIG ===== #

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data_files" / "Enterprise Department"

MERCHANT_FILE = RAW_DIR / "merchant_data.csv"
STAFF_FILE = RAW_DIR / "staff_data.csv"
ORDER_FILES = [
    RAW_DIR / "order_with_merchant_data1.csv",
    RAW_DIR / "order_with_merchant_data2.csv",
    RAW_DIR / "order_with_merchant_data3.csv",
]

OUT_DIR = PROJECT_ROOT / "clean_data" / "enterprise"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ===== SHARED CLEANING HELPERS ===== #


def normalize_phone(phone: str) -> str:
    """
    Normalize phone numbers to a consistent format.

    - Keep only digits.
    - If 11 digits starting with '1' -> drop leading 1.
    - If 10 digits -> XXX-XXX-XXXX.
    - Otherwise -> return digits string as-is.
    """
    if pd.isna(phone):
        return ""

    digits = re.sub(r"\D", "", str(phone))

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

    return digits


def clean_merchant_name(name: str) -> str:
    """
    Remove inconsistent outer quotation marks and extra spaces.
    E.g. '"Ontodia, Inc"' -> 'Ontodia, Inc'
         ' "Whitby Group" ' -> 'Whitby Group'
    """
    if pd.isna(name):
        return ""

    cleaned = str(name).strip()
    cleaned = cleaned.strip('"').strip("'")       # remove outer quotes
    cleaned = re.sub(r"\s+", " ", cleaned)        # collapse multiple spaces
    return cleaned

# ===== LOAD + CLEAN FUNCTIONS ===== #


def load_merchant_data(path: Path) -> pd.DataFrame:
    """Load merchant_data.csv and apply name / phone / country cleaning."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Types first
    df["merchant_id"] = df["merchant_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["contact_number"] = df["contact_number"].astype(str)

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # Apply same cleaning as merchant_clean.py
    df["country"] = "United States"
    df["name"] = df["name"].apply(clean_merchant_name)
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    return df


def load_staff_data(path: Path) -> pd.DataFrame:
    """Load staff_data.csv and apply phone / country cleaning."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Types first
    df["staff_id"] = df["staff_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["job_level"] = df["job_level"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["contact_number"] = df["contact_number"].astype(str)

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # Apply same cleaning as staff_clean.py
    df["country"] = "United States"
    df["contact_number"] = df["contact_number"].apply(normalize_phone)

    return df


def load_orders_with_merchant_data(paths) -> pd.DataFrame:
    """Load and combine order_with_merchant_data1/2/3.csv."""
    frames = []

    for path in paths:
        df = pd.read_csv(path)

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        df["order_id"] = df["order_id"].astype(str)
        df["merchant_id"] = df["merchant_id"].astype(str)
        df["staff_id"] = df["staff_id"].astype(str)

        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    combined = combined.drop_duplicates(
        subset=["order_id", "merchant_id", "staff_id"])

    return combined

# ===== SAVE ===== #


def save_outputs(df: pd.DataFrame, name: str):
    """Save both CSV and Parquet with a consistent naming pattern."""
    csv_path = OUT_DIR / f"{name}_clean.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"Saved {name}: {len(df):,} rows")
    print(f"  CSV:     {csv_path}")
    print(f"  Parquet: {parquet_path}\n")

# ===== MAIN ===== #


def main():
    print("=== Ingesting Enterprise department datasets ===\n")

    # merchant_data
    print("Loading merchant_data.csv ...")
    merchants = load_merchant_data(MERCHANT_FILE)
    print(merchants.head())
    print(merchants.dtypes, "\n")
    save_outputs(merchants, "merchant_data")

    # staff_data
    print("Loading staff_data.csv ...")
    staff = load_staff_data(STAFF_FILE)
    print(staff.head())
    print(staff.dtypes, "\n")
    save_outputs(staff, "staff_data")

    # order_with_merchant_data1/2/3
    print("Loading order_with_merchant_data*.csv ...")
    orders_merchant_staff = load_orders_with_merchant_data(ORDER_FILES)
    print(orders_merchant_staff.head())
    print(orders_merchant_staff.dtypes, "\n")
    save_outputs(orders_merchant_staff, "order_with_merchant_data_all")

    print("Enterprise ingestion complete.")


if __name__ == "__main__":
    main()
