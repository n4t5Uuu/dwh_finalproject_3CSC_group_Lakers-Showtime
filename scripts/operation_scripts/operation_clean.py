import pandas as pd
from pathlib import Path

# ================== CONFIG ================== #
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

RAW_DIR = PROJECT_ROOT / "data_files" / "Operations Department"

OUT_DIR = Path("clean_data") / "operations"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Order and line item raw slices
ORDER_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20211001.csv",
    "order_data_20211001-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
]

LINE_ITEM_FILES = [
    "line_item_data_prices1.csv",
    "line_item_data_prices2.csv",
    "line_item_data_prices3.csv",
]

ORDER_DELAYS_FILE = RAW_DIR / "order_delays.csv"
# ================== CONFIG ================== #


def load_concat_files(file_names: list[str]) -> pd.DataFrame:
    """Load multiple CSVs from RAW_DIR and vertically concatenate them."""
    dfs = []
    for fname in file_names:
        src_path = RAW_DIR / fname
        if not src_path.exists():
            print(f"[WARN] File not found (skipped): {src_path}")
            continue

        print(f"Loading {src_path.name}")
        df = pd.read_csv(src_path)

        # Drop index column if present
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        dfs.append(df)

    if not dfs:
        print("[WARN] No files loaded for this group.")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ========== ORDERS CLEANING ========== #
def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean combined order_data_* dataframe."""
    if df.empty:
        return df

    # Normalize 'estimated arrival' if present
    if "estimated arrival" in df.columns:
        df["estimated_arrival_in_days"] = (
            df["estimated arrival"]
            .astype(str)
            .str.replace("days", "", regex=False)
            .str.strip()
            .fillna("0")
        )
        # Coerce to int (if something weird sneaks in, it'll become NaN then filled)
        df["estimated_arrival_in_days"] = (
            pd.to_numeric(df["estimated_arrival_in_days"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        df = df.drop(columns=["estimated arrival"])

    return df


def save_orders(df: pd.DataFrame) -> None:
    if df.empty:
        print("[INFO] No order data to save.")
        return

    out_csv = OUT_DIR / "order_data_clean.csv"
    out_parquet = OUT_DIR / "order_data_clean.parquet"

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)

    print(f"[OK] Saved cleaned orders to: {out_csv} and {out_parquet}")


# ========== LINE ITEM CLEANING ========== #
def clean_line_items(df: pd.DataFrame) -> pd.DataFrame:
    """Clean combined line_item_data_prices* dataframe."""
    if df.empty:
        return df

    # Normalize 'quantity' if present -> fact_order_quantity
    if "quantity" in df.columns:
        df["fact_order_quantity"] = (
            df["quantity"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)  # grab numeric part
            .fillna("0")
        )

        df["fact_order_quantity"] = (
            pd.to_numeric(df["fact_order_quantity"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

        df = df.drop(columns=["quantity"])

    return df


def save_line_items(df: pd.DataFrame) -> None:
    if df.empty:
        print("[INFO] No line item data to save.")
        return

    out_csv = OUT_DIR / "line_item_data_clean.csv"
    out_parquet = OUT_DIR / "line_item_data_clean.parquet"

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)

    print(f"[OK] Saved cleaned line items to: {out_csv} and {out_parquet}")


# ========== ORDER DELAYS CLEANING ========== #
def clean_order_delays(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for order_delays.csv."""
    if df.empty:
        return df

    # Drop index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Add more rules here if you need (e.g., parsing delay columns)
    return df


def save_order_delays(df: pd.DataFrame) -> None:
    if df.empty:
        print("[INFO] No order_delays data to save.")
        return

    out_csv = OUT_DIR / "order_delays_clean.csv"
    out_parquet = OUT_DIR / "order_delays_clean.parquet"

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)

    print(f"[OK] Saved cleaned order delays to: {out_csv} and {out_parquet}")


# ========== MAIN ========== #
def main():
    # Orders: load all order_data_* and combine
    orders_raw = load_concat_files(ORDER_FILES)
    orders_clean = clean_orders(orders_raw)
    save_orders(orders_clean)

    # Line items: load all line_item_data_prices* and combine
    line_items_raw = load_concat_files(LINE_ITEM_FILES)
    line_items_clean = clean_line_items(line_items_raw)
    save_line_items(line_items_clean)

    # Order delays: single file
    if ORDER_DELAYS_FILE.exists():
        print(f"Loading {ORDER_DELAYS_FILE.name}")
        od_df = pd.read_csv(ORDER_DELAYS_FILE)
        od_clean = clean_order_delays(od_df)
        save_order_delays(od_clean)
    else:
        print(f"[WARN] order_delays file not found: {ORDER_DELAYS_FILE}")


if __name__ == "__main__":
    main()
