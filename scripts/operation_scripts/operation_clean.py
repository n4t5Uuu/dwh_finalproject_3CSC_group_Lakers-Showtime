import pandas as pd
from pathlib import Path

# ============================================================
#                     PATH CONFIGURATION
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent

RAW_DIR = Path("/data_files/Operations Department")
OUT_DIR = Path("/clean_data/operations")
ENTERPRISE_DIR = Path("/clean_data/enterprise")
USER_DATA_CLEAN = Path("/clean_data/customer_management/user_data.csv")

OUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
#            DATE KEY HELPER (NEW – REQUIRED)
# ============================================================
def to_date_key(dt: pd.Series) -> pd.Series:
    """
    Convert datetime into YYYYMMDD integer key.
    NaT -> <NA>
    """
    return dt.dt.strftime("%Y%m%d").astype("Int64")


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

MERCHANT_STAFF_MAIN = ENTERPRISE_DIR / "order_with_merchant_data_all.csv"
MERCHANT_STAFF_ISSUES = ENTERPRISE_DIR / "order_with_merchant_data_all_issues.csv"


# ============================================================
#                     LOADING FUNCTIONS
# ============================================================
def load_concat_files(file_list):
    dfs = []
    for fname in file_list:
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
#                     ORDERS CLEANING
# ============================================================
def clean_orders(df):
    if df.empty:
        return df

    # --- Estimated arrival ---
    if "estimated arrival" in df.columns:
        df["estimated_arrival_in_days"] = (
            df["estimated arrival"]
            .astype(str)
            .str.replace("days", "", regex=False)
            .str.replace("day", "", regex=False)
            .str.strip()
            .str.extract(r"(\d+)", expand=False)
            .fillna("0")
            .astype("int64")
        )
        df = df.drop(columns=["estimated arrival"])

    # --- Parse transaction_date + create date_key (NEW) ---
    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        df["transaction_date_key"] = to_date_key(df["transaction_date"])
    else:
        df["transaction_date_key"] = pd.NA

    return df


# ============================================================
#                  ORDER DELAYS CLEANING
# ============================================================
def clean_order_delays(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    possible_cols = ["delay_in_days", "delay in days", "delay", "delay(days)", "delay_days"]
    delay_col = None
    for col in possible_cols:
        if col in df.columns:
            delay_col = col
            break

    if delay_col:
        df["delay_in_days"] = (
            df[delay_col]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("0")
            .astype("int64")
        )
        if delay_col != "delay_in_days":
            df = df.drop(columns=[delay_col])
    else:
        df["delay_in_days"] = 0

    return df


# ============================================================
#                LINE ITEM CLEANING + JOIN
# ============================================================
def clean_prices(df):
    if df.empty:
        return df

    if "quantity" in df.columns:
        df["quantity_clean"] = (
            df["quantity"].astype(str).str.extract(r"(\d+)", expand=False).fillna("0").astype(int)
        )
        df = df.drop(columns=["quantity"])

    return df


def clean_products(df):
    if df.empty:
        return df

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    df = df.dropna(subset=["order_id", "product_id"])
    return df


def combine_duplicate_line_items(df):
    if df.empty:
        return df

    if "quantity_clean" not in df.columns:
        print("[WARN] quantity_clean missing — cannot combine duplicates.")
        return df

    group_cols = [c for c in df.columns if c != "quantity_clean"]

    combined = (
        df.groupby(group_cols, as_index=False)["quantity_clean"]
        .sum()
    )

    return combined


def join_prices_products(price_df, product_df):
    if price_df.empty or product_df.empty:
        print("[WARN] Cannot join line items — missing dataset.")
        return pd.DataFrame()

    price_df["row_num"] = price_df.groupby("order_id").cumcount() + 1
    product_df["row_num"] = product_df.groupby("order_id").cumcount() + 1

    merged = price_df.merge(product_df, on=["order_id", "row_num"], how="inner")
    merged = merged.drop(columns=["row_num"])

    merged = combine_duplicate_line_items(merged)

    return merged


# ============================================================
#        USER KEY FIX — SELECT CORRECT KEY USING CREATION DATE
# ============================================================
def build_best_user_key_map(user_df):
    if "creation_date" not in user_df.columns:
        print("[WARN] user_data has no creation_date; using first occurrence only.")
        return user_df.groupby("user_id")["user_key"].first()

    user_df["creation_date"] = pd.to_datetime(user_df["creation_date"], errors="coerce")

    best = (
        user_df.sort_values("creation_date")
        .groupby("user_id")["user_key"]
        .first()
    )

    return best


def assign_user_keys_to_orders(orders_df, user_df):
    if orders_df.empty:
        return orders_df

    print("Building best user_key map (based on earliest creation_date)...")
    best_map = build_best_user_key_map(user_df)

    print("Merging user_key into orders using user_id...")
    merged = orders_df.merge(best_map.rename("user_key"), on="user_id", how="left")

    merged["user_key"] = merged["user_key"].fillna("U_UNKNOWN-0")
    return merged


# ============================================================
#                     SAVE HELPERS
# ============================================================
def save_csv_parquet(df, name):
    csv_path = OUT_DIR / f"{name}.csv"
    parquet_path = OUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"[OK] Saved {name} → {csv_path}")


# ============================================================
#                        MAIN PIPELINE
# ============================================================
def main():

    # ---------------- ORDERS CLEANING ----------------
    orders_raw = load_concat_files(ORDER_FILES)
    orders_clean = clean_orders(orders_raw)

    # ---------------- ORDER DELAYS -------------------
    if ORDER_DELAYS_FILE.exists():
        print("Loading order_delays.csv...")
        od_df = pd.read_csv(ORDER_DELAYS_FILE)
        od_clean = clean_order_delays(od_df)

        orders_final = orders_clean.merge(od_clean, on="order_id", how="left")
        orders_final["delay_in_days"] = (
            orders_final["delay_in_days"].fillna(0).astype(int)
        )
    else:
        print("[WARN] No order_delays.csv found — delay set to 0")
        orders_final = orders_clean.copy()
        orders_final["delay_in_days"] = 0

    save_csv_parquet(orders_final, "orders_final")

    # ---------------- USER KEY MERGE -----------------
    if USER_DATA_CLEAN.exists():
        print("Loading user_data.csv for user_key mapping...")
        user_clean = pd.read_csv(USER_DATA_CLEAN)
        orders_enhanced = assign_user_keys_to_orders(orders_final, user_clean)
    else:
        print("[WARN] No user_data found — user_key defaults to UNKNOWN")
        orders_enhanced = orders_final.copy()
        orders_enhanced["user_key"] = "U_UNKNOWN-0"

    save_csv_parquet(orders_enhanced, "orders_final_enhanced")


    # ---------------- MERCHANT & STAFF MERGE ---------
    enterprise_frames = []

    if MERCHANT_STAFF_MAIN.exists():
        enterprise_frames.append(pd.read_csv(MERCHANT_STAFF_MAIN))

    if MERCHANT_STAFF_ISSUES.exists():
        enterprise_frames.append(pd.read_csv(MERCHANT_STAFF_ISSUES))

    if enterprise_frames:
        merchant_staff_df = pd.concat(enterprise_frames, ignore_index=True)

        merchant_staff_df["merchant_key"] = merchant_staff_df["merchant_key"].fillna("").astype(str)
        merchant_staff_df["staff_key"] = merchant_staff_df["staff_key"].fillna("").astype(str)

        merchant_staff_df.loc[
            merchant_staff_df["merchant_key"] == "",
            "merchant_key"
        ] = "MERCHANT_UNKNOWN-0"

        orders_ready = orders_enhanced.merge(
            merchant_staff_df,
            on="order_id",
            how="left"
        )

        # -----------------------------------------------------
        # FIX DUPLICATE DATE KEYS (x/y) + FIX FLOAT DECIMALS
        # -----------------------------------------------------
        if "transaction_date_key_x" in orders_ready.columns:
            orders_ready = orders_ready.rename(
                columns={"transaction_date_key_x": "transaction_date_key"}
            )
        if "transaction_date_key_y" in orders_ready.columns:
            orders_ready = orders_ready.drop(columns=["transaction_date_key_y"])

        # Convert date_key to Int64 (remove decimals)
        if "transaction_date_key" in orders_ready.columns:
            orders_ready["transaction_date_key"] = orders_ready["transaction_date_key"].astype("Int64")

        # -----------------------------------------------------

        # --- FINAL TYPE FIXES (remove decimals permanently) ---
        
        int_cols = [
            "estimated_arrival_in_days",
            "transaction_date_key",
            "delay_in_days"
        ]

        for col in int_cols:
            if col in orders_ready.columns:
                orders_ready[col] = (
                    pd.to_numeric(orders_ready[col], errors="coerce")
                    .astype("Int64")
                )


        orders_ready["merchant_key"] = (
            orders_ready["merchant_key"]
            .replace("", "MERCHANT_UNKNOWN-0")
            .fillna("MERCHANT_UNKNOWN-0")
        )

        orders_ready["staff_key"] = (
            orders_ready["staff_key"]
            .replace("", "STAFF_UNKNOWN-0")
            .fillna("STAFF_UNKNOWN-0")
        )

    else:
        print("[WARN] No enterprise merchant/staff mapping found")
        orders_ready = orders_enhanced.copy()
        orders_ready["merchant_key"] = "MERCHANT_UNKNOWN-0"
        orders_ready["staff_key"] = "STAFF_UNKNOWN-0"


    # ============================================================
    #   SPLIT CLEAN ROWS VS. ISSUE ROWS
    # ============================================================
    unknown_mask = (
        (orders_ready["user_key"] == "U_UNKNOWN-0") |
        (orders_ready["merchant_key"] == "MERCHANT_UNKNOWN-0") |
        (orders_ready["staff_key"] == "STAFF_UNKNOWN-0") |
        (orders_ready["transaction_date_key"].isna())
    )

    orders_issues = orders_ready[unknown_mask].copy()
    orders_clean_only = orders_ready[~unknown_mask].copy()

    save_csv_parquet(orders_clean_only, "orders_final_ready")
    save_csv_parquet(orders_issues, "orders_final_ready_issues")

    print(f"[INFO] Clean rows saved: {len(orders_clean_only):,}")
    print(f"[INFO] Issue rows saved: {len(orders_issues):,}")


    # ---------------- LINE ITEMS ---------------------
    price_raw = load_concat_files(LINE_ITEM_PRICE_FILES)
    price_clean = clean_prices(price_raw)

    product_raw = load_concat_files(PRODUCT_FILES)
    product_clean = clean_products(product_raw)

    line_items_final = join_prices_products(price_clean, product_clean)
    save_csv_parquet(line_items_final, "line_item_final")

    print("\n[DONE] Operation Department cleaning pipeline completed.")


if __name__ == "__main__":
    main()
