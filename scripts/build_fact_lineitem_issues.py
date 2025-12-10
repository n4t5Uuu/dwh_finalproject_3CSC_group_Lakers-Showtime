import pandas as pd
from pathlib import Path

# ============================================================
#                     PATH CONFIGURATION
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Define paths relative to the Project Root
RAW_OPS_DIR = PROJECT_ROOT / "data_files" / "Operations Department"
CLEAN_OPS_DIR = PROJECT_ROOT / "clean_data" / "operations"
FACTS_DIR = PROJECT_ROOT / "clean_data" / "facts"
OUT_DIR = FACTS_DIR 

# Input Files
FACT_LINEITEM_FILE = FACTS_DIR / "factLineItem.csv"

# WE NEED BOTH ORDER FILES TO FIND ALL KEYS
ORDERS_READY_FILE = CLEAN_OPS_DIR / "orders_final_ready.csv" 
ORDERS_ISSUES_FILE = CLEAN_OPS_DIR / "orders_final_ready_issues.csv"

PRODUCT_FILES = [
    "line_item_data_products1.csv",
    "line_item_data_products2.csv",
    "line_item_data_products3.csv",
]

# Output File
OUTPUT_FILE = OUT_DIR / "factLineItem_issues.csv"

# ============================================================
#                        MAIN LOGIC
# ============================================================

def load_raw_products():
    """Concatenates all raw product files for comparison."""
    dfs = []
    for fname in PRODUCT_FILES:
        path = RAW_OPS_DIR / fname
        if path.exists():
            print(f"Loading raw product file: {fname}")
            df = pd.read_csv(path)
            if "Unnamed: 0" in df.columns:
                df = df.drop(columns=["Unnamed: 0"])
            dfs.append(df)
        else:
            print(f"[WARN] Missing file: {path}")
    
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def get_all_orders():
    """Loads and combines both clean and issue orders to get a full key map."""
    orders_list = []
    
    if ORDERS_READY_FILE.exists():
        print(f"Loading {ORDERS_READY_FILE.name}...")
        orders_list.append(pd.read_csv(ORDERS_READY_FILE))
        
    if ORDERS_ISSUES_FILE.exists():
        print(f"Loading {ORDERS_ISSUES_FILE.name}...")
        orders_list.append(pd.read_csv(ORDERS_ISSUES_FILE))
        
    if not orders_list:
        raise FileNotFoundError("No order files found in clean_data/operations/")
        
    full_orders = pd.concat(orders_list, ignore_index=True)
    return full_orders

def main():
    print("\n========== BUILDING LINE ITEM ISSUES ==========\n")

    # 1. Load Data
    raw_products = load_raw_products()
    if raw_products.empty:
        print("No raw products found. Exiting.")
        return

    if not FACT_LINEITEM_FILE.exists():
        raise FileNotFoundError(f"Missing {FACT_LINEITEM_FILE}. Run build_fact_orders.py first.")
    
    fact_df = pd.read_csv(FACT_LINEITEM_FILE)
    
    # LOAD ALL ORDERS (Valid + Issues)
    orders_df = get_all_orders() 

    # Ensure Merge Keys are Strings
    raw_products["order_id"] = raw_products["order_id"].astype(str)
    raw_products["product_id"] = raw_products["product_id"].astype(str)
    fact_df["order_id"] = fact_df["order_id"].astype(str)
    fact_df["product_id"] = fact_df["product_id"].astype(str)
    orders_df["order_id"] = orders_df["order_id"].astype(str)

    # 2. Compare Raw Products vs. Clean Fact Table
    print("Comparing Raw Products vs. Clean Fact Table...")
    merged_check = raw_products.merge(
        fact_df[["order_id", "product_id"]].drop_duplicates(),
        on=["order_id", "product_id"],
        how="left",
        indicator=True
    )

    issues_df = merged_check[merged_check["_merge"] == "left_only"].drop(columns=["_merge"]).copy()
    print(f"Found {len(issues_df):,} product rows dropped from final fact table.")

    # 3. Enrich with Context (Using ALL ORDERS)
    print("Enriching issues with keys from All Orders...")
    
    order_keys_lookup = orders_df[[
        "order_id", 
        "user_key", 
        "merchant_key", 
        "staff_key", 
        "transaction_date_key"
    ]].rename(columns={"transaction_date_key": "date_key"})

    # Deduplicate orders just in case (though they should be unique across the two files)
    order_keys_lookup = order_keys_lookup.drop_duplicates(subset=["order_id"])

    # Merge
    final_issues = issues_df.merge(
        order_keys_lookup, 
        on="order_id", 
        how="left"
    )
    
    # 4. Save
    final_issues.to_csv(OUTPUT_FILE, index=False)
    print(f"\n[OK] Saved issues file to: {OUTPUT_FILE}")

    # DEBUG PREVIEW
    print("\n--- PREVIEW OF GENERATED ISSUES ---")
    # Show rows where we actually found a merchant key to prove it works
    preview_df = final_issues.dropna(subset=["merchant_key"])
    if not preview_df.empty:
        print(preview_df[["order_id", "merchant_key", "date_key"]].head())
    else:
        print("Still seeing NaNs? Here is the raw head:")
        print(final_issues[["order_id", "merchant_key", "date_key"]].head())

if __name__ == "__main__":
    main()