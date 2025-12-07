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

def assign_user_keys_to_orders(orders_df: pd.DataFrame, user_data_df: pd.DataFrame):
    """
    Match each order to the correct user_key using temporal logic.
    - If duplicate user_ids exist, assign based on creation_date vs transaction_date.
    """

    # Normalize ID fields
    orders_df["user_id"] = orders_df["user_id"].astype(str)
    user_data_df["user_id_orig"] = user_data_df["user_id_orig"].astype(str)

    # Convert dates
    orders_df["transaction_date"] = pd.to_datetime(orders_df["transaction_date"], errors="coerce")
    user_data_df["creation_date"] = pd.to_datetime(user_data_df["creation_date"], errors="coerce")

    merged_list = []

    # Process each order user_id separately for performance
    for uid, order_group in orders_df.groupby("user_id"):
        user_subset = user_data_df[user_data_df["user_id_orig"] == uid].copy()

        if user_subset.empty:
            # No match at all → leave user_key = None
            order_group["user_key"] = None
            merged_list.append(order_group)
            continue

        # If EXACTLY one user matches → easy
        if len(user_subset) == 1:
            order_group["user_key"] = user_subset["user_key"].iloc[0]
            merged_list.append(order_group)
            continue

        # Multiple users → temporal matching
        user_subset = user_subset.sort_values("creation_date")

        def pick_user_key(row):
            # Find all user versions created before the order date
            valid = user_subset[user_subset["creation_date"] <= row["transaction_date"]]

            if len(valid) > 0:
                # pick the most recent version before order
                return valid.iloc[-1]["user_key"]
            else:
                # order occurs before any user record — pick earliest version
                return user_subset.iloc[0]["user_key"]

        order_group["user_key"] = order_group.apply(pick_user_key, axis=1)
        merged_list.append(order_group)

    # Combine enhanced orders
    final = pd.concat(merged_list, ignore_index=True)
    return final



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
            df["quantity"].astype(str).str.extract(r"(\d+)", expand=False).fillna("0")
        )

        df["quantity_clean"] = (
            pd.to_numeric(df["quantity_clean"], errors="coerce").fillna(0).astype(int)
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
#                     SAVE HELPERS
# ============================================================

def save_csv_parquet(df: pd.DataFrame, name: str):
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"[OK] Saved: {csv_path} and {parquet_path}")


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
    #        NEW: USER-KEY ASSIGNMENT (Customer Management)
    # ==========================================================

    # Path to cleaned user_data from Customer Management Dept
    USER_CLEAN_PATH = PROJECT_ROOT / "clean_data" / "customer_management" / "user_data.csv"

    if USER_CLEAN_PATH.exists():
        print("Loading user_data clean file for user_key assignment...")
        user_data_clean = pd.read_csv(USER_CLEAN_PATH)

        # Assign the correct user_key to each order
        print("Assigning user_key to orders using temporal logic...")
        orders_final_enhanced = assign_user_keys_to_orders(orders_final, user_data_clean)

        # Save enhanced version
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
