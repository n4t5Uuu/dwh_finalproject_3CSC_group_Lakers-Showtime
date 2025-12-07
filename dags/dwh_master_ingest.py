import sys
import traceback

from loaders.load_dim_date import load_dim_date
from loaders.load_dim_user import load_dim_user
from loaders.load_dim_merchant import load_dim_merchant
from loaders.load_dim_staff import load_dim_staff
from loaders.load_dim_campaign import load_dim_campaign

from loaders.load_fact_orders import load_fact_orders
from loaders.load_fact_line_item import load_fact_line_item
from loaders.load_fact_campaign_availed import load_fact_campaign_availed


# ============================================================
#                  SIMPLE RUNNER HELPER
# ============================================================
def run_step(name, func, *args, **kwargs):
    print(f"\n===== RUNNING: {name} =====\n")
    try:
        func(*args, **kwargs)
        print(f"[OK] {name} completed.\n")
    except Exception as e:
        print(f"[ERROR] {name} failed!")
        traceback.print_exc()
        sys.exit(1)   # stop the whole ingestion pipeline


# ============================================================
#                        MAIN PIPELINE
# ============================================================
def main():

    print("\n\n=========== STARTING DWH MASTER INGEST ===========\n")

    # 1. DIMENSIONS FIRST  -----------------------------------

    run_step(
        "Load dimDate",
        load_dim_date,
        file_path="/clean_data/date/dim_date.csv"
    )

    run_step(
        "Load dimUser",
        load_dim_user,
        file_path="/clean_data/customer_management/user_data.csv"
    )

    run_step(
        "Load dimMerchant",
        load_dim_merchant,
        file_path="/clean_data/enterprise/merchant_data.csv"
    )

    run_step(
        "Load dimStaff",
        load_dim_staff,
        file_path="/clean_data/enterprise/staff_data.csv"
    )

    run_step(
        "Load dimCampaign",
        load_dim_campaign,
        file_path="/clean_data/marketing/campaign_data.csv"
    )


    # 2. FACT TABLES -----------------------------------------

    run_step(
        "Load factOrders",
        load_fact_orders,
        file_path="/clean_data/operations/orders_final_ready.csv"
    )

    run_step(
        "Load factLineItem",
        load_fact_line_item,
        file_path="/clean_data/operations/line_item_final.csv"
    )

    run_step(
        "Load factCampaignAvailed",
        load_fact_campaign_availed,
        file_path="/clean_data/marketing/transactional_campaign_data.csv"
    )


    print("\n\n=========== DWH MASTER INGEST COMPLETE ===========\n")


if __name__ == "__main__":
    main()
