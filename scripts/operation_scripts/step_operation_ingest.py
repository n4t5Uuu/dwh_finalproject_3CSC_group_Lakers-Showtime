import pandas as pd
from pathlib import Path

# FIXED: Use absolute path for Docker volume mount
RAW_DIR = Path("/data_files/Operations Department")

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

def clean_and_validate(src_path: Path, cleaned_dir: Path):
    print(f"Processing {src_path.name}")
    df = pd.read_csv(src_path)
    # Drop index column
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)
    duplicate_mask = pd.Series(False, index=df.index)
    null_mask = df.isnull().any(axis=1)
    invalid_numeric_mask = pd.Series(False, index=df.index)
    # Line items and custom file checks here (add your logic as before)
    cleaned_path = cleaned_dir / src_path.name
    df_clean = df[~duplicate_mask & ~null_mask & ~invalid_numeric_mask]
    df_clean.to_csv(cleaned_path, index=False)
    print(f"Saved cleaned file to {cleaned_path}")

def main():
    for fname in OPERATION_FILES:
        src_path = RAW_DIR / fname
        if not src_path.exists():
            print(f"File not found: {src_path}")
            continue
        clean_and_validate(src_path, CLEANED_DIR)

if __name__ == "__main__":
    main()
