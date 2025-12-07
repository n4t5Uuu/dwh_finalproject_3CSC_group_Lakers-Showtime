import pandas as pd
from pathlib import Path

# ============================================================
#                     CORRECT PATH SETUP
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

RAW_PATH = PROJECT_ROOT / "data_files" / "Operations Department"
CLEAN_PATH = PROJECT_ROOT / "clean_data" / "operations"

CLEAN_PATH.mkdir(parents=True, exist_ok=True)

PRICE_FILES = [
    "line_item_data_prices1.csv",
    "line_item_data_prices2.csv",
    "line_item_data_prices3.csv",
]

PRODUCT_FILES = [
    "line_item_data_products1.csv",
    "line_item_data_products2.csv",
    "line_item_data_products3.csv",
]


# ============================================================
#                       LOADERS
# ============================================================

def load_multiple_csv(files: list[str]) -> pd.DataFrame:
    dfs = []
    for fname in files:
        file_path = RAW_PATH / fname
        if not file_path.exists():
            print(f"[WARN] Missing file: {file_path}")
            continue

        print(f"Loading: {file_path}")
        df = pd.read_csv(file_path)

        # Remove auto index
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ============================================================
#                       CLEANERS
# ============================================================

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Clean price/quantity side of line item data."""
    if df.empty:
        return df

    if "quantity" in df.columns:
        df["quantity_clean"] = (
            df["quantity"].astype(str).str.extract(r"(\d+)", expand=False).fillna("0")
        )

        df["quantity_clean"] = (
            pd.to_numeric(df["quantity_clean"], errors="coerce").fillna(0).astype("int64")
        )

        df = df.drop(columns=["quantity"])

    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean product line items."""
    if df.empty:
        return df

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df = df.dropna(subset=["order_id", "product_id"])

    return df


# ============================================================
#                 SAFE JOIN (NO DUPLICATES)
# ============================================================

def join_price_and_product(price_df: pd.DataFrame, product_df: pd.DataFrame) -> pd.DataFrame:
    """Merge datasets using row_num to avoid cross joins."""
    if price_df.empty or product_df.empty:
        print("[WARN] Cannot join empty datasets.")
        return pd.DataFrame()

    print("Assigning row numbers per order_id...")

    price_df = price_df.copy()
    product_df = product_df.copy()

    price_df["row_num"] = price_df.groupby("order_id").cumcount() + 1
    product_df["row_num"] = product_df.groupby("order_id").cumcount() + 1

    print("Joining line items using order_id + row_num...")

    merged = price_df.merge(
        product_df,
        on=["order_id", "row_num"],
        how="inner"
    )

    merged = merged.drop(columns=["row_num"])

    return merged


# ============================================================
#                       SAVER
# ============================================================

def save_clean_line_items(df: pd.DataFrame) -> None:
    out_path = CLEAN_PATH / "line_item_data_clean.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[OK] Saved cleaned and joined line item data → {out_path}")


# ============================================================
#                       MAIN
# ============================================================

def main():
    # Load raw data
    raw_prices = load_multiple_csv(PRICE_FILES)
    raw_products = load_multiple_csv(PRODUCT_FILES)

    # Clean data
    price_clean = clean_prices(raw_prices)
    product_clean = clean_products(raw_products)

    # Join safely
    final_df = join_price_and_product(price_clean, product_clean)

    # Save output
    save_clean_line_items(final_df)


if __name__ == "__main__":
    main()
