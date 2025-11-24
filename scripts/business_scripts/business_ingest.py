import pandas as pd
from pathlib import Path

INPUT_FILE = Path("product_list.csv")

OUTPUT_DIR = Path("ingested")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / "product_list_clean.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "product_list.parquet"


def load_product_list(path: Path) -> pd.DataFrame:
    """Load raw product list CSV and do minimal cleanup."""
    df = pd.read_csv(path)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])


    df["product_id"] = df["product_id"].astype(str)
    df["product_name"] = df["product_name"].astype(str)
    df["product_type"] = df["product_type"].astype(str)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


def main():
    print(f"Reading: {INPUT_FILE}")

    df = load_product_list(INPUT_FILE)

    print("Sample rows after ingestion:")
    print(df.head())
    print("\nColumn dtypes:")
    print(df.dtypes)

    # Save outputs
    df.to_csv(OUTPUT_CSV, index=False)
    df.to_parquet(OUTPUT_PARQUET, index=False)

    print(f"\nIngestion complete ✅")
    print(f"Rows: {len(df):,}")
    print(f"Saved CSV to:      {OUTPUT_CSV}")
    print(f"Saved Parquet to:  {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()