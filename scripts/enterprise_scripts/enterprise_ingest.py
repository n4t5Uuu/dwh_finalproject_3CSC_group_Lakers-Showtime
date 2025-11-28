
import pandas as pd
from pathlib import Path

RAW_DIR = Path(".")  

MERCHANT_FILE = RAW_DIR / "merchant_data.csv"
STAFF_FILE = RAW_DIR / "staff_data.csv"
ORDER_FILES = [
    RAW_DIR / "order_with_merchant_data1.csv",
    RAW_DIR / "order_with_merchant_data2.csv",
    RAW_DIR / "order_with_merchant_data3.csv",
]

OUT_DIR = Path("ingested") / "enterprise"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_merchant_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean merchant_data.csv"""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["merchant_id"] = df["merchant_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["contact_number"] = df["contact_number"].astype(str)

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    return df


def load_staff_data(path: Path) -> pd.DataFrame:
    """Load and minimally clean staff_data.csv"""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df["staff_id"] = df["staff_id"].astype(str)
    df["name"] = df["name"].astype(str)
    df["job_level"] = df["job_level"].astype(str)
    df["street"] = df["street"].astype(str)
    df["state"] = df["state"].astype(str)
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    df["contact_number"] = df["contact_number"].astype(str)

    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    return df


def load_orders_with_merchant_data(paths) -> pd.DataFrame:
    """Load and combine order_with_merchant_data1/2/3.csv"""
    frames = []

    for path in paths:
        df = pd.read_csv(path)

        # Some files have Unnamed: 0, some don't
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        df["order_id"] = df["order_id"].astype(str)
        df["merchant_id"] = df["merchant_id"].astype(str)
        df["staff_id"] = df["staff_id"].astype(str)

        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    combined = combined.drop_duplicates(subset=["order_id", "merchant_id", "staff_id"])

    return combined


def save_outputs(df: pd.DataFrame, name: str):
    """Save both CSV and Parquet with a consistent naming pattern."""
    csv_path = OUT_DIR / f"{name}_clean.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"Saved {name}: {len(df):,} rows")
    print(f"  CSV:     {csv_path}")
    print(f"  Parquet: {parquet_path}\n")


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

    print("Enterprise ingestion complete ✅")


if __name__ == "__main__":
    main()