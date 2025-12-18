from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import sys
from psycopg2.extras import execute_values

sys.path.append("/scripts")
from build_fact_line_item_src import main as build_fact_line_item_src

def load_csv_to_staging(table, csv_path, columns):
    df = pd.read_csv(csv_path)
    df = df[columns]  

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(f"TRUNCATE staging.{table};")

    execute_values(
        cur,
        f"""
        INSERT INTO staging.{table} ({", ".join(columns)})
        VALUES %s
        """,
        df.itertuples(index=False, name=None)
    )

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="dag_stage_all_sources",
    start_date=datetime(2025, 1, 1),
    template_searchpath=["/sql"],
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "staging", "kimball"],
) as dag:

    # CREATE STAGING TABLES
    create_all_staging_tables = PostgresOperator(
        task_id="create_all_staging_tables",
        postgres_conn_id="postgres_default",
        sql="/create_all_staging_tables.sql",
    )

    # LOAD USER DATA
    customer_management_load = PythonOperator(
        task_id="load_staging_user_data_all",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "user_data_all",
            "csv_path": "/clean_data/customer_management/user_data_all.csv",
            "columns": [
                "user_id",
                "name",
                "creation_date",
                "birthdate",
                "gender",
                "street",
                "city",
                "state",
                "country",
                "device_address",
                "user_type",
                "job_title",
                "job_level",
                "credit_card_number",
                "issuing_bank",
            ],
        },
    )



    # LOAD CAMPAIGN DATA
    campaign_load = PythonOperator(
        task_id="load_staging_campaign",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "campaign_data",
            "csv_path": "/clean_data/marketing/campaign_data.csv",
            "columns": [
                "campaign_id",
                "campaign_name",
                "campaign_description",
                "discount_pct",
            ],
        },
    )


    # LOAD PRODUCT LIST
    product_load = PythonOperator(
        task_id="load_staging_product_list",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "product_list",
            "csv_path": "/clean_data/business/product_list_clean.csv",
            "columns": [
                "product_id",
                "product_name",
                "product_type",
                "price",
            ],
        },
    )


    # LOAD STAFF DATA
    staff_load = PythonOperator(
        task_id="load_staging_staff",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "staff_data",
            "csv_path": "/clean_data/enterprise/staff_data.csv",
            "columns": [
                "staff_id",
                "name",
                "job_level",
                "street",
                "state",
                "city",
                "country",
                "contact_number",
                "creation_date",
                "staff_creation_date_key",
            ],
        },
    )


    # LOAD MERCHANT DATA
    merchant_load = PythonOperator(
        task_id="load_staging_merchant",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "merchant_data",
            "csv_path": "/clean_data/enterprise/merchant_data.csv",
            "columns": [
                "merchant_id",
                "name",
                "street",
                "state",
                "city",
                "country",
                "contact_number",
                "creation_date",
                "merchant_creation_date_key",
            ],
        },
    )


    # LOAD TRANSACTIONAL CAMPAIGN DATA
    transactional_campaign_load = PythonOperator(
    task_id="load_staging_transactional_campaign_clean",
    python_callable=load_csv_to_staging,
    op_kwargs={
        "table": "transactional_campaign_clean",
        "csv_path": "/clean_data/marketing/transactional_campaign_clean.csv",
        "columns": [
            "order_id",
            "campaign_id",
            "transaction_date",
            "date_key",
            "estimated_arrival_days",
            "availed",
        ],
    },
)

    # LOAD ORDERS DATA
    orders_load = PythonOperator(
        task_id="load_staging_orders_clean",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "orders_clean",
            "csv_path": "/clean_data/operations/orders_clean.csv",
            "columns": [
                "order_id",
                "user_id",
                "transaction_date",
                "date_key",
                "estimated_arrival_days",
            ],
        },
    )

    # LOAD ORDER DELAYS DATA
    order_delays_load = PythonOperator(
        task_id="load_staging_order_delays_clean",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "order_delays_clean",
            "csv_path": "/clean_data/operations/order_delays_clean.csv",
            "columns": [
                "order_id",
                "delay_in_days",
            ],
        },
    )

    # LOAD ENTERPRISE ORDER WITH MERCHANT DATA
    enterprise_order_merchant_load = PythonOperator(
        task_id="load_staging_order_with_merchant_clean",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "order_with_merchant_clean",
            "csv_path": "/clean_data/enterprise/order_with_merchant_clean.csv",
            "columns": [
                "order_id",
                "merchant_id",
                "staff_id",
            ],
        },
    )

    # BUILD FACT LINE ITEM SRC
    fact_line_item_src_build = PythonOperator(
        task_id="build_fact_line_item_src",
        python_callable=build_fact_line_item_src,
    )

    # LOAD FACT LINE ITEM SRC
    fact_line_item_src_load = PythonOperator(
        task_id="load_staging_fact_line_item_src",
        python_callable=load_csv_to_staging,
        op_kwargs={
            "table": "fact_line_item_src",
            "csv_path": "/clean_data/facts/fact_line_item_src.csv",
            "columns": [
                "order_id",
                "product_id",
                "product_name",
                "user_id",
                "merchant_id",
                "staff_id",
                "unit_price",
                "quantity",
                "line_amount",
                "date_key",
                "campaign_id",
            ],
        },
    )


    # DEPENDENCIES
    create_all_staging_tables >> [customer_management_load, campaign_load, product_load, 
                                  staff_load, merchant_load, transactional_campaign_load, 
                                  enterprise_order_merchant_load, 
                                  orders_load, order_delays_load
                                  ]
    fact_line_item_src_build >> fact_line_item_src_load


    # [
    #     customer_management_load,
    #     campaign_load,
    #     product_load,
    #     staff_load,
    #     merchant_load,
    #     enterprise_order_merchant_load,
    #     orders_load,
    #     order_delays_load,
    # ] >> fact_line_item_src_build

    # fact_line_item_src_build >> fact_line_item_src_create >> fact_line_item_src_load


