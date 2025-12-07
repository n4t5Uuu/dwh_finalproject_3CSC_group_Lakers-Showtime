import pandas as pd
from pathlib import Path

# ============================================================
#                     CONFIGURATION
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

RAW_DIR = PROJECT_ROOT / "data_files" / "Operations Department"
OUT_DIR = PROJECT_ROOT / "clean_data" / "operations"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
#                     RAW FILE LISTS
# ============================================================

ORDER_FILES = [
    "order_data_20200101-20200701.csv",
    "order_data_20200701-20211001.csv",
    "order_data_20211001-20220101.csv",
    "order_data_20220101-20221201.csv",
    "order_data_20221201-20230601.csv",
    "order_data_20230601-20240101.csv",
]

LINE_ITEM_PRICE_FILES = [
    "line_item_data_prices1.csv",
    "line_item_data_prices2.csv",
    "line_item_data_prices3.csv",
]

PRODUCT_FILES = [
    "line_item_data_products1.csv",
    "line_item_data_products2.csv",
    "line_item_data_products3.csv",
]

ORDER_DELAYS_FILE = RAW_DIR / "order_delays.csv"


# ============================================================
#                   UTILITY FUNCTIONS
# ============================================================

def load_concat_files(files: list[str]) -> pd.DataFrame:
    dfs = []
    for fname in files:
        path = RAW_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing file: {path}")
            continue

        print(f"Loading {path.name}")
        df = pd.read_csv(path)

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def save_csv_parquet(df: pd.DataFrame, name: str):
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"[OK] Saved: {csv_path} and {parquet_path}")


# ============================================================
#                  ORDERS CLEANING
# ============================================================

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean order datasets; extract estimated arrival days."""
    if df.empty:
        return df

    if "estimated arrival" in df.columns:
        df["estimated_arrival_in_days"] = (
            df["estimated arrival"]
            .astype(str)
            .str.replace("days", "", regex=False)
            .str.replace("day", "", regex=False)
            .str.replace(" ", "")
            .str.extract(r"(\d+)", expand=False)
            .fillna("0")
        )

        df["estimated_arrival_in_days"] = (
            pd.to_numeric(df["estimated_arrival_in_days"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

        df = df.drop(columns=["estimated arrival"])

    return df


def assign_user_keys_to_orders(orders_df: pd.DataFrame, user_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach user_key to each order using BOTH:
    - user_data.creation_date
    - orders.transaction_date

    Logic:
    1) For each (user_id_orig, transaction_date):
       choose the latest user row where creation_date <= transaction_date.
    2) If no creation_date <= transaction_date exists for that user_id_orig,
       fall back to the earliest creation_date for that user_id_orig.
    """

    if orders_df.empty:
        return orders_df

    # ---- Normalize IDs ----
    orders = orders_df.copy()
    users = user_data_df.copy()

    # Orders use original user_id (e.g. "USER40678")
    orders["user_id"] = orders["user_id"].astype(str)
    orders["transaction_date"] = pd.to_datetime(
        orders["transaction_date"], errors="coerce"
    )

    # user_data_clean from Customer Management has:
    #  - user_id_orig (original id)
    #  - creation_date
    #  - user_key
    users["user_id_orig"] = users["user_id_orig"].astype(str)
    users["creation_date"] = pd.to_datetime(
        users["creation_date"], errors="coerce"
    )

    # We'll join using the *original* user_id coming from orders
    orders["user_id_orig"] = orders["user_id"]

    # Only keep needed columns for the SCD-style join
    users_scd = users[["user_id_orig", "creation_date", "user_key"]].copy()

    # 🔑 Important: sort by the "on" keys only for merge_asof
    users_scd = users_scd.sort_values("creation_date")
    orders = orders.sort_values("transaction_date")

    # ---- Primary rule: SCD-like as-of join ----
    merged = pd.merge_asof(
        orders,
        users_scd,
        left_on="transaction_date",
        right_on="creation_date",
        by="user_id_orig",
        direction="backward",       # creation_date <= transaction_date
        allow_exact_matches=True,
    )

    # ---- Fallback: if no creation_date <= transaction_date for that user ----
    mask_missing = merged["user_key"].isna()

    if mask_missing.any():
        earliest = (
            users_scd
            .sort_values("creation_date")
            .drop_duplicates(subset=["user_id_orig"], keep="first")
            [["user_id_orig", "user_key"]]
            .rename(columns={"user_key": "fallback_user_key"})
        )

        merged = merged.merge(earliest, on="user_id_orig", how="left")

        merged.loc[mask_missing, "user_key"] = merged.loc[
            mask_missing, "fallback_user_key"
        ]

        merged = merged.drop(columns=["fallback_user_key"])

    # ---- Drop helper columns you don't want in the final fact ----
    cols_to_drop = [c for c in ["user_id_orig",
                                "creation_date"] if c in merged.columns]
    merged = merged.drop(columns=cols_to_drop)

    return merged


# ============================================================
#                  ORDER DELAY CLEANING
# ============================================================

def clean_order_delays(df: pd.DataFrame) -> pd.DataFrame:
    """Clean order_delays.csv and standardize column names."""
    if df.empty:
        return df

    # Remove unwanted index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Fix inconsistent column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Now the delay column should be named 'delay_in_days'
    if "delay_in_days" in df.columns:
        df["delay_in_days"] = (
            pd.to_numeric(df["delay_in_days"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
    else:
        # If missing entirely, create it
        df["delay_in_days"] = 0

    return df


# ============================================================
#                  LINE ITEM CLEANING
# ============================================================

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Clean price and quantity for line items."""
    if df.empty:
        return df

    if "quantity" in df.columns:
        df["quantity_clean"] = (
            df["quantity"].astype(str).str.extract(
                r"(\d+)", expand=False).fillna("0")
        )

        df["quantity_clean"] = (
            pd.to_numeric(df["quantity_clean"],
                          errors="coerce").fillna(0).astype(int)
        )

        df = df.drop(columns=["quantity"])

    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean product side of line items."""
    if df.empty:
        return df

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df = df.dropna(subset=["order_id", "product_id"])

    return df


# ============================================================
#         FIXED JOIN: MATCH LINE ITEMS SAFELY (NO DUPES)
# ============================================================

def join_prices_products(price_df: pd.DataFrame, product_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty or product_df.empty:
        print("[WARN] Missing line item dataset → cannot join.")
        return pd.DataFrame()

    print("Assigning row numbers per order_id to avoid cross joins...")

    # Assign row number per order_id so line items align properly
    price_df = price_df.copy()
    product_df = product_df.copy()

    price_df["row_num"] = price_df.groupby("order_id").cumcount() + 1
    product_df["row_num"] = product_df.groupby("order_id").cumcount() + 1

    print("Joining price and product tables on order_id + row_num...")

    merged = price_df.merge(
        product_df,
        on=["order_id", "row_num"],
        how="inner"
    )

    merged = merged.drop(columns=["row_num"])

    return merged


# ============================================================
#                     MAIN PIPELINE
# ============================================================

def main():

    # --------------------- ORDERS -------------------------
    orders_raw = load_concat_files(ORDER_FILES)
    orders_clean = clean_orders(orders_raw)

    # --------------------- ORDER DELAYS --------------------
    if ORDER_DELAYS_FILE.exists():
        print(f"Loading {ORDER_DELAYS_FILE.name}")
        od_df = pd.read_csv(ORDER_DELAYS_FILE)
        od_clean = clean_order_delays(od_df)
    else:
        print("[WARN] No order_delays.csv found.")
        od_clean = pd.DataFrame()

    # --------------------- MERGE ORDERS + DELAYS ----------
    if not od_clean.empty:
        orders_final = orders_clean.merge(od_clean, on="order_id", how="left")

        # Default NULL delays to 0
        if "delay_in_days" in orders_final.columns:
            orders_final["delay_in_days"] = (
                orders_final["delay_in_days"]
                .fillna(0)
                .astype("int64")
            )
    else:
        orders_final = orders_clean

    # Save original cleaned Operations orders
    save_csv_parquet(orders_final, "orders_final")

    # ==========================================================
    #        USER-KEY ASSIGNMENT (Customer Management)
    # ==========================================================

    USER_CLEAN_PATH = PROJECT_ROOT / "clean_data" / \
        "customer_management" / "user_data.csv"

    if USER_CLEAN_PATH.exists():
        print("Loading user_data clean file for user_key assignment...")
        user_data_clean = pd.read_csv(USER_CLEAN_PATH)

        print("Assigning user_key to orders using temporal logic (creation_date vs transaction_date)...")
        orders_final_enhanced = assign_user_keys_to_orders(
            orders_final, user_data_clean)

        save_csv_parquet(orders_final_enhanced, "orders_final_enhanced")
    else:
        print("[WARN] No user_data.csv found → cannot assign user_key to orders.")

    # --------------------- LINE ITEM PRICES ---------------
    price_raw = load_concat_files(LINE_ITEM_PRICE_FILES)
    price_clean = clean_prices(price_raw)

    # --------------------- LINE ITEM PRODUCTS -------------
    product_raw = load_concat_files(PRODUCT_FILES)
    product_clean = clean_products(product_raw)

    # --------------------- MERGE LINE ITEMS ---------------
    line_items_final = join_prices_products(price_clean, product_clean)
    save_csv_parquet(line_items_final, "line_item_final")

    print("[DONE] Operation Department cleaning pipeline finished.")


if __name__ == "__main__":
    main()
