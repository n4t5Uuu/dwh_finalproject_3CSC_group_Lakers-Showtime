from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# =====================================================
# IMPORT CLEANING SCRIPTS
# =====================================================

sys.path.append("/scripts")

from operation_scripts.orders_clean import main as clean_orders
from operation_scripts.line_item_prices_clean import main as clean_line_item_prices
from operation_scripts.order_delays_clean import main as clean_order_delays
from operation_scripts.line_item_products_clean import main as clean_line_item_products

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_clean_operations",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "cleaning", "operations"],
) as dag:

    # -------------------------------------------------
    # CLEAN ORDERS
    # -------------------------------------------------
    clean_orders_task = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders,
    )

    # -------------------------------------------------
    # CLEAN LINE ITEM PRODUCTS
    # -------------------------------------------------
    clean_line_item_products_task = PythonOperator(
        task_id="clean_line_item_products",
        python_callable=clean_line_item_products,
    )

    # -------------------------------------------------
    # CLEAN LINE ITEM PRICES
    # -------------------------------------------------
    clean_line_item_prices_task = PythonOperator(
        task_id="clean_line_item_prices",
        python_callable=clean_line_item_prices,
    )

    # -------------------------------------------------
    # CLEAN ORDER DELAYS
    # -------------------------------------------------
    clean_order_delays_task = PythonOperator(
        task_id="clean_order_delays",
        python_callable=clean_order_delays,
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    clean_orders_task >> [
        clean_line_item_products_task,
        clean_line_item_prices_task,
        clean_order_delays_task
    ]
