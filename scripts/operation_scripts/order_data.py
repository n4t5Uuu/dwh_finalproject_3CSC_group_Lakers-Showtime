import pandas as pd
from pathlib import Path

DATA_DIR = Path("data_files/Operations Department")
ORDER_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20211001.csv",
    "order_data_20211001-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
]

def clean_order_data():
    for fname in ORDER_FILES:
        path = DATA_DIR / fname
        print(f"Cleaning {path}...")
        df = pd.read_csv(path)
        if "estimated arrival" in df.columns:
            df["estimated arrival"] = (
                df["estimated arrival"]
                .astype(str)
                .str.replace("days", "", regex=False)
                .str.strip()
                .fillna(0)
                .astype("int64")
            )
            df.rename(columns={"estimated arrival": "estimated_arrival_in_days"}, inplace=True)
            df.to_csv(path, index=False)
            print(f"Cleaned {path}")
        else:
            print(f"SKIPPED {path}: No 'estimated arrival' column found.")

if __name__ == "__main__":
    clean_order_data()
