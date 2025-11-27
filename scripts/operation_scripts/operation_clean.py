import pandas as pd
from pathlib import Path

RAW_DIR = Path("data_files/Operations Department")
CLEANED_DIR = Path("Cleaned Folder/Operations Department")
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

OPERATION_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20211001.csv",
    "order_data_20211001-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
    "line_item_data_prices1.csv",
    "line_item_data_prices2.csv",
    "line_item_data_prices3.csv",
    "order_delays.csv"
]

def clean_file(src_path: Path, cleaned_dir: Path):
    print(f"Processing {src_path.name}")
    df = pd.read_csv(src_path)

    # Drop index column if present
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)

    # Line item cleaning: Clean 'quantity' inconsistent formats if present
    if src_path.name.startswith("line_item_data_prices") and "quantity" in df.columns:
        df["fact_order_quantity"] = (
            df["quantity"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna(0)
            .astype("int64")
        )
        df.drop(columns=["quantity"], inplace=True)

    # Order data cleaning: Clean 'estimated arrival' if present (strip 'days', make int, rename)
    if src_path.name.startswith("order_data") and "estimated arrival" in df.columns:
        df["estimated_arrival_in_days"] = (
            df["estimated arrival"]
            .astype(str)
            .str.replace("days", "", regex=False)
            .str.strip()
            .fillna(0)
            .astype("int64")
        )
        df.drop(columns=["estimated arrival"], inplace=True)

    # Save cleaned CSV and Parquet
    cleaned_csv = cleaned_dir / (src_path.stem + "_clean.csv")
    cleaned_parquet = cleaned_dir / (src_path.stem + ".parquet")
    df.to_csv(cleaned_csv, index=False)
    df.to_parquet(cleaned_parquet, index=False)
    print(f"Saved cleaned files to: {cleaned_csv}, {cleaned_parquet}\n")

def main():
    for fname in OPERATION_FILES:
        src_path = RAW_DIR / fname
        if src_path.exists():
            clean_file(src_path, CLEANED_DIR)
        else:
            print(f"File not found: {src_path}")

if __name__ == "__main__":
    main()
