from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import sys
import pandas as pd
from psycopg2.extras import execute_values

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

# FACT SOURCE
from build_fact_line_item_src import main as build_fact_line_item_src

# STAGING FUNCTION
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
    dag_id="dag_shopzada_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/sql"],
    tags=["shopzada", "dwh", "kimball"],
) as dag:


    # CLEAN ALL SOURCES
    with TaskGroup("clean_all_sources") as clean_group:

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




    with TaskGroup("stage_all_sources") as stage_group:

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
                                  ] >> fact_line_item_src_build
    fact_line_item_src_build >> fact_line_item_src_load

    with TaskGroup("build_all_dim") as dim_group:

        create_schema_shopzada = PostgresOperator(
                task_id="create_schema_shopzada",
                postgres_conn_id="postgres_default",
                sql="""
                CREATE SCHEMA IF NOT EXISTS shopzada;
                """
            )

        create_dim_indexes = PostgresOperator(
            task_id="create_dim_indexes",
            postgres_conn_id="postgres_default",
            sql="""
            -- USER SCD2
            CREATE INDEX IF NOT EXISTS idx_dim_user_scd
                ON shopzada.dim_user (user_id, effective_from, effective_to);

            -- STAFF SCD2
            CREATE INDEX IF NOT EXISTS idx_dim_staff_scd
                ON shopzada.dim_staff (staff_id, effective_from, effective_to);

            -- MERCHANT SCD2
            CREATE INDEX IF NOT EXISTS idx_dim_merchant_scd
                ON shopzada.dim_merchant (merchant_id, effective_from, effective_to);
            """
        )


        build_dim_campaign = PostgresOperator(
            task_id="build_dim_campaign",
            postgres_conn_id="postgres_default",
            sql="/build_dim_campaign.sql",
        )

        build_dim_date = PostgresOperator(
            task_id="build_dim_date",
            postgres_conn_id="postgres_default",
            sql="/build_dim_date.sql",
        )

        build_dim_merchant = PostgresOperator(
            task_id="build_dim_merchant",
            postgres_conn_id="postgres_default",
            sql="/build_dim_merchant.sql",
        )

        build_dim_user = PostgresOperator(
            task_id="build_dim_user",
            postgres_conn_id="postgres_default",
            sql="/build_dim_user.sql",
        )

        build_dim_product = PostgresOperator(
            task_id="build_dim_product",
            postgres_conn_id="postgres_default",
            sql="/build_dim_product.sql",
        )

        build_dim_staff = PostgresOperator(
            task_id="build_dim_staff",
            postgres_conn_id="postgres_default",
            sql="/build_dim_staff.sql",
        )

        create_schema_shopzada >> [
            build_dim_campaign, build_dim_date, build_dim_merchant, 
            build_dim_user, build_dim_product, build_dim_staff
            ] >> create_dim_indexes

        build_dim_date >> build_dim_product >> build_dim_user



    with TaskGroup("build_all_facts") as fact_group:

        create_fact_orders = PostgresOperator(
            task_id="create_fact_orders",
            postgres_conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS shopzada.fact_orders (
                    fact_orders_key BIGSERIAL PRIMARY KEY,

                    order_id VARCHAR(100) NOT NULL,

                    user_key INT NOT NULL,
                    merchant_key INT NOT NULL DEFAULT 0,
                    staff_key INT NOT NULL DEFAULT 0,
                    date_key INT NOT NULL,

                    order_amount NUMERIC(14,2) NOT NULL,
                    estimated_arrival_days INT,
                    delay_in_days INT,

                    campaign_key INT NOT NULL DEFAULT -1,   -- -1 = no campaign
                    availed_flag INT NOT NULL DEFAULT 0,    -- 0/1
                    discount_pct INT,
                    discount_amount NUMERIC(14,2)
                );
            """
        )


        load_fact_orders = PostgresOperator(
            task_id="load_fact_orders",
            postgres_conn_id="postgres_default",
            sql="""
            TRUNCATE shopzada.fact_orders;

            INSERT INTO shopzada.fact_orders (
                order_id,
                user_key,
                merchant_key,
                staff_key,
                date_key,
                order_amount,
                estimated_arrival_days,
                delay_in_days,
                campaign_key,
                availed_flag,
                discount_pct,
                discount_amount
            )
            SELECT
                li.order_id,

                li.user_key,
                li.merchant_key,
                li.staff_key,
                li.date_key,

                SUM(li.line_amount) AS order_amount,

                o.estimated_arrival_days,
                d.delay_in_days,

                -- CAMPAIGN FIELDS
                CASE
                    WHEN NULLIF(NULLIF(tc.campaign_id, ''), 'NaN') IS NULL
                        THEN -1
                    ELSE COALESCE(dc.campaign_key, 0)
                END AS campaign_key,

                COALESCE(tc.availed, 0) AS availed_flag,

                dc.discount_pct,

                CASE
                    WHEN COALESCE(tc.availed, 0) = 1
                    AND dc.discount_pct IS NOT NULL
                    THEN ROUND(
                        SUM(li.line_amount) * (dc.discount_pct / 100.0),
                        2
                    )
                    ELSE 0.00
                END AS discount_amount

            FROM shopzada.fact_line_item li

            -- ORDER DETAILS
            LEFT JOIN staging.orders_clean o
                ON li.order_id = o.order_id

            LEFT JOIN staging.order_delays_clean d
                ON li.order_id = d.order_id

            -- CAMPAIGN (order-level, at most one per order)
            LEFT JOIN staging.transactional_campaign_clean tc
                ON li.order_id = tc.order_id

            LEFT JOIN shopzada.dim_campaign dc
                ON tc.campaign_id = dc.campaign_id

            GROUP BY
                li.order_id,
                li.user_key,
                li.merchant_key,
                li.staff_key,
                li.date_key,
                o.estimated_arrival_days,
                d.delay_in_days,
                tc.campaign_id,
                tc.availed,
                dc.campaign_key,
                dc.discount_pct;

            """
        )

        create_fact_table = PostgresOperator(
                task_id="create_fact_line_item",
                postgres_conn_id="postgres_default",
                sql="""
                CREATE SCHEMA IF NOT EXISTS shopzada;

                CREATE TABLE IF NOT EXISTS shopzada.fact_line_item (
                    fact_line_item_key BIGSERIAL PRIMARY KEY,

                    order_id VARCHAR(100) NOT NULL,

                    user_key INT NOT NULL DEFAULT 0,
                    merchant_key INT NOT NULL DEFAULT 0,
                    staff_key INT NOT NULL DEFAULT 0,
                    product_key INT NOT NULL DEFAULT 0,
                    date_key INT NOT NULL,

                    unit_price NUMERIC(12,2) NOT NULL,
                    quantity INT NOT NULL,
                    line_amount NUMERIC(14,2) NOT NULL
                );
                """
            )

            # -------------------------------------------------
            # LOAD FACT TABLE (SCD2-SAFE)
            # -------------------------------------------------
        load_fact_table = PostgresOperator(
            task_id="load_fact_line_item",
            postgres_conn_id="postgres_default",
            sql="""
            TRUNCATE shopzada.fact_line_item;

            INSERT INTO shopzada.fact_line_item (
                order_id,
                user_key,
                merchant_key,
                staff_key,
                product_key,
                date_key,
                unit_price,
                quantity,
                line_amount
            )
                SELECT
                    src.order_id,

                    -- USER (SCD2 SAFE + handles txn before first effective_from)
                    COALESCE(u.user_key, 0) AS user_key,

                    -- MERCHANT (SCD2 SAFE + handles txn before first effective_from)
                    COALESCE(m.merchant_key, 0) AS merchant_key,

                    -- STAFF (SCD2 SAFE + handles txn before first effective_from)
                    COALESCE(s.staff_key, 0) AS staff_key,

                    -- PRODUCT (single-row pick: composite preferred, else product_id)
                    COALESCE(p.product_key, 0) AS product_key,

                    src.date_key,
                    src.unit_price,
                    src.quantity,
                    src.line_amount

                FROM staging.fact_line_item_src src

                -- Make a real DATE once, so comparisons are consistent
                CROSS JOIN LATERAL (
                    SELECT to_date(src.date_key::text, 'YYYYMMDD') AS txn_date
                ) d

                -- ---------------- USER SCD2 JOIN ----------------
                LEFT JOIN LATERAL (
                    SELECT u.user_key
                    FROM shopzada.dim_user u
                    WHERE u.user_id = src.user_id
                    AND (
                            -- normal in-range match
                            (d.txn_date >= u.effective_from
                            AND (u.effective_to IS NULL OR d.txn_date <= u.effective_to))
                            OR
                            -- txn before first known effective_from -> map to earliest version
                            (d.txn_date < (
                                SELECT MIN(effective_from)
                                FROM shopzada.dim_user
                                WHERE user_id = src.user_id
                            ))
                    )
                    ORDER BY u.effective_from
                    LIMIT 1
                ) u ON TRUE

                -- ---------------- MERCHANT SCD2 JOIN ----------------
                LEFT JOIN LATERAL (
                    SELECT m.merchant_key
                    FROM shopzada.dim_merchant m
                    WHERE m.merchant_id = src.merchant_id
                    AND (
                            (d.txn_date >= m.effective_from
                            AND (m.effective_to IS NULL OR d.txn_date <= m.effective_to))
                            OR
                            (d.txn_date < (
                                SELECT MIN(effective_from)
                                FROM shopzada.dim_merchant
                                WHERE merchant_id = src.merchant_id
                            ))
                    )
                    ORDER BY m.effective_from
                    LIMIT 1
                ) m ON TRUE

                -- ---------------- STAFF SCD2 JOIN ----------------
                LEFT JOIN LATERAL (
                    SELECT s.staff_key
                    FROM shopzada.dim_staff s
                    WHERE s.staff_id = src.staff_id
                    AND (
                            (d.txn_date >= s.effective_from
                            AND (s.effective_to IS NULL OR d.txn_date <= s.effective_to))
                            OR
                            (d.txn_date < (
                                SELECT MIN(effective_from)
                                FROM shopzada.dim_staff
                                WHERE staff_id = src.staff_id
                            ))
                    )
                    ORDER BY s.effective_from
                    LIMIT 1
                ) s ON TRUE

                -- ---------------- PRODUCT PICK (NO MULTIPLYING ROWS) ----------------
                LEFT JOIN LATERAL (
                    SELECT p.product_key
                    FROM shopzada.dim_product p
                    WHERE p.product_id = src.product_id
                    ORDER BY
                        -- prefer exact composite match if possible
                        CASE
                            WHEN src.product_name IS NOT NULL
                                AND p.product_name = src.product_name THEN 0
                            ELSE 1
                        END,
                        -- optional: if you have a "last_updated" or similar, prefer newest
                        p.product_key DESC
                    LIMIT 1
                ) p ON TRUE
                ;
                """
            )

        create_fact_campaign = PostgresOperator(
            task_id="create_fact_campaign_availed",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE SCHEMA IF NOT EXISTS shopzada;

            CREATE TABLE IF NOT EXISTS shopzada.fact_campaign_availed (
                fact_campaign_key SERIAL PRIMARY KEY,

                order_id VARCHAR(100) NOT NULL,

                campaign_key INT NOT NULL,
                user_key INT NOT NULL,
                merchant_key INT NOT NULL,
                staff_key INT NOT NULL,
                date_key INT NOT NULL,

                availed INT NOT NULL,
                discount_pct INT,

                CONSTRAINT uq_fact_campaign UNIQUE (order_id, campaign_key, date_key)
            );
            """
        )

        load_fact_campaign = PostgresOperator(
            task_id="load_fact_campaign_availed",
            postgres_conn_id="postgres_default",
            sql="""
            TRUNCATE shopzada.fact_campaign_availed;

            INSERT INTO shopzada.fact_campaign_availed (
                order_id,
                campaign_key,
                user_key,
                merchant_key,
                staff_key,
                date_key,
                availed,
                discount_pct
            )
            SELECT
                tc.order_id,
                
                CASE
                    WHEN NULLIF(NULLIF(tc.campaign_id, ''), 'NaN') IS NULL THEN -1 -- Not Applicable
                    ELSE COALESCE(dc.campaign_key, 0)        -- Unknown (late-arriving)
                END AS campaign_key,

                COALESCE(du.user_key, 0)      AS user_key,
                COALESCE(dm.merchant_key, 0)  AS merchant_key,
                COALESCE(ds.staff_key, 0)     AS staff_key,
                tc.date_key,
                tc.availed,
                dc.discount_pct
            FROM staging.transactional_campaign_clean tc

            -- CAMPAIGN DIM 
            LEFT JOIN shopzada.dim_campaign dc
                ON tc.campaign_id = dc.campaign_id

            -- ORDER -> USER
            LEFT JOIN staging.orders_clean o
                ON tc.order_id = o.order_id

            LEFT JOIN LATERAL (
                SELECT du.user_key
                FROM shopzada.dim_user du
                WHERE du.user_id = o.user_id
                AND (
                        -- normal SCD2 match
                        TO_DATE(tc.date_key::text, 'YYYYMMDD')
                            BETWEEN du.effective_from
                                AND COALESCE(du.effective_to, DATE '9999-12-31')

                        -- fallback: transaction before first version
                    OR TO_DATE(tc.date_key::text, 'YYYYMMDD') <
                        (
                            SELECT MIN(effective_from)
                            FROM shopzada.dim_user
                            WHERE user_id = o.user_id
                        )
                )
                ORDER BY du.effective_from
                LIMIT 1
            ) du ON TRUE



            -- ORDER -> MERCHANT / STAFF
            LEFT JOIN staging.order_with_merchant_clean om
                ON tc.order_id = om.order_id

            LEFT JOIN LATERAL (
                SELECT dm.merchant_key
                FROM shopzada.dim_merchant dm
                WHERE dm.merchant_id = om.merchant_id
                AND (
                        TO_DATE(tc.date_key::text, 'YYYYMMDD')
                            BETWEEN dm.effective_from
                                AND COALESCE(dm.effective_to, DATE '9999-12-31')

                    OR TO_DATE(tc.date_key::text, 'YYYYMMDD') <
                        (
                            SELECT MIN(effective_from)
                            FROM shopzada.dim_merchant
                            WHERE merchant_id = om.merchant_id
                        )
                )
                ORDER BY dm.effective_from
                LIMIT 1
            ) dm ON TRUE


            LEFT JOIN LATERAL (
                SELECT ds.staff_key
                FROM shopzada.dim_staff ds
                WHERE ds.staff_id = om.staff_id
                AND (
                        TO_DATE(tc.date_key::text, 'YYYYMMDD')
                            BETWEEN ds.effective_from
                                AND COALESCE(ds.effective_to, DATE '9999-12-31')

                    OR TO_DATE(tc.date_key::text, 'YYYYMMDD') <
                        (
                            SELECT MIN(effective_from)
                            FROM shopzada.dim_staff
                            WHERE staff_id = om.staff_id
                        )
                )
                ORDER BY ds.effective_from
                LIMIT 1
            ) ds ON TRUE

            """
        )

        create_fact_orders >> load_fact_orders
        create_fact_table >> load_fact_table 
        create_fact_campaign >> load_fact_campaign 
        

    clean_group >> stage_group >> dim_group >> fact_group
