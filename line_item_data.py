import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1] / "data_files" / "Operations Department"

PRICE_FILES = [
    "line_item_data_prices1.csv",
    "line_item_data_prices2.csv",
    "line_item_data_prices3.csv",
]

def clean_line_item_prices():
    for fname in PRICE_FILES:
        path = BASE_DIR / fname
        df = pd.read_csv(path)

        # keep only digits from quantity (2pcs, 2px, 2pieces -> 2)
        df["quantity"] = (
            df["quantity"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .astype("int64")
        )

        df.to_csv(path, index=False)
        print(f"Cleaned {path}")

if __name__ == "__main__":
    clean_line_item_prices()
