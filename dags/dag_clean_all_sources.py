from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/scripts")

# DIRECTORY SETUP
from setup_directories import main as setup_directories
from convert_all_to_csv import main as convert_to_csv

# BUSINESS
from business_scripts.business_clean import main as clean_business

# CUSTOMER MANAGEMENT
from customer_management_scripts.customer_management_clean import main as clean_customer

# OPERATIONS
from operation_scripts.orders_clean import main as clean_orders
from operation_scripts.line_item_products_clean import main as clean_line_item_products
from operation_scripts.line_item_prices_clean import main as clean_line_item_prices
from operation_scripts.order_delays_clean import main as clean_order_delays

# MARKETING
from marketing_scripts.transactional_campaign_clean import main as clean_transactional_campaign
from marketing_scripts.campaign_clean import main as clean_campaign

# ENTERPRISE
from enterprise_scripts.order_with_merchant_clean import main as clean_order_with_merchant
from enterprise_scripts.merchant_clean import main as clean_merchant
from enterprise_scripts.staff_clean import main as clean_staff


with DAG(
    dag_id="dag_clean_all_sources",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "cleaning", "kimball"],
) as dag:

    convert_to_csv = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_to_csv,
    )

    # BUSINESS CLEANING
    clean_business_task = PythonOperator(
        task_id="clean_business",
        python_callable=clean_business,
    )


    # CUSTOMER MANAGEMENT CLEANING
    clean_customer_task = PythonOperator(
        task_id="clean_customer_management",
        python_callable=clean_customer,
    )


    # OPERATIONS CLEANING
    clean_orders_task = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders,
    )

    clean_line_item_products_task = PythonOperator(
        task_id="clean_line_item_products",
        python_callable=clean_line_item_products,
    )

    clean_line_item_prices_task = PythonOperator(
        task_id="clean_line_item_prices",
        python_callable=clean_line_item_prices,
    )

    clean_order_delays_task = PythonOperator(
        task_id="clean_order_delays",
        python_callable=clean_order_delays,
    )


    # MARKETING CLEANING
    clean_transactional_campaign_task = PythonOperator(
        task_id="clean_transactional_campaign",
        python_callable=clean_transactional_campaign,
    )
    clean_campaign_task = PythonOperator(
        task_id="clean_campaign",
        python_callable=clean_campaign,
    )


    # ENTERPRISE CLEANING
    clean_order_with_merchant_task = PythonOperator(
        task_id="clean_order_with_merchant",
        python_callable=clean_order_with_merchant,
    )
    clean_merchant_task = PythonOperator(
        task_id="clean_merchant",
        python_callable=clean_merchant,
    )
    clean_staff_task = PythonOperator(
        task_id="clean_staff",
        python_callable=clean_staff,
    )


    # TASK DEPENDENCIES
    # setup_directories >> clean_business_task
    convert_to_csv >> [
        clean_business_task,
        clean_staff_task,
        clean_customer_task,
        clean_orders_task,
        clean_line_item_products_task,
        clean_line_item_prices_task,
        clean_order_delays_task,
        clean_transactional_campaign_task,
        clean_campaign_task,
        clean_order_with_merchant_task,
        clean_merchant_task
    ]



