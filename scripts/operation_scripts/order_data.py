import pandas as pd
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[1] / "data_files" / "Operations Department"

ORDER_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20210101.csv",
    "order_data_20210101-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
]


def clean_order_data():
    for fname in ORDER_FILES:
        path = DATA_DIR / fname
        print(f"Cleaning {path}...")

        df = pd.read_csv(path)

        # 1. remove the text "days"
        # 2. strip whitespace
        # 3. convert to integer
        df["estimated_arrival"] = (
            df["estimated_arrival"]
            .astype(str)
            .str.replace("days", "", regex=False)
            .str.strip()
            .astype("int64")
        )

        # rename column
        df = df.rename(columns={"estimated_arrival": "estimated_arrival_in_days"})

        # overwrite original CSV
        df.to_csv(path, index=False)

        print(f"Finished {path}")


if __name__ == "__main__":
    clean_order_data()
