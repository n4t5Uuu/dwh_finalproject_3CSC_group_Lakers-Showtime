import pandas as pd
from pathlib import Path


RAW_PATH = Path("data_files/operations")
CLEAN_PATH = Path("clean_data/operations")
CLEAN_PATH.mkdir(parents=True, exist_ok=True)


def load_line_item_data() -> pd.DataFrame:
    file_path = RAW_PATH / "line_item_data.csv"
    df = pd.read_csv(file_path)
    return df


def clean_line_item_data(df: pd.DataFrame) -> pd.DataFrame:
    # Example cleaning – adapt to your assignment rules
    df = df.drop_duplicates()
    df = df.dropna(subset=["order_id", "product_id"])
    return df


def save_clean_line_item_data(df: pd.DataFrame) -> None:
    out_path = CLEAN_PATH / "line_item_data_clean.parquet"
    df.to_parquet(out_path, index=False)


def main():
    df = load_line_item_data()
    df_clean = clean_line_item_data(df)
    save_clean_line_item_data(df_clean)


if __name__ == "__main__":
    main()
