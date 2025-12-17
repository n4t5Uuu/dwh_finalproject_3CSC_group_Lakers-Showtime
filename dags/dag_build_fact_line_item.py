from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


with DAG(
    dag_id="dag_build_fact_line_item",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "fact", "line_item"],
) as dag:


    create_fact_table = PostgresOperator(
        task_id="create_fact_line_item",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.fact_line_item (
            fact_line_item_key BIGSERIAL PRIMARY KEY,

            order_id VARCHAR(100) NOT NULL,

            user_key INT NOT NULL,
            merchant_key INT NOT NULL,
            staff_key INT NOT NULL,
            product_key INT NOT NULL,
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

            -- USER (SCD2 SAFE)
            COALESCE(u.user_key, 0) AS user_key,

            -- MERCHANT (SCD2 SAFE)
            COALESCE(m.merchant_key, 0) AS merchant_key,

            -- STAFF (SCD2 SAFE)
            COALESCE(s.staff_key, 0) AS staff_key,

            -- PRODUCT (TYPE 1, COMPOSITE NATURAL KEY)
            COALESCE(p.product_key, 0) AS product_key,

            src.date_key,
            src.unit_price,
            src.quantity,
            src.line_amount

        FROM staging.fact_line_item_src src

        -- ---------------- USER SCD2 JOIN ----------------
        LEFT JOIN LATERAL (
            SELECT u.user_key
            FROM shopzada.dim_user u
            WHERE u.user_id = src.user_id
            AND (
                    src.date_key >= TO_CHAR(u.effective_from, 'YYYYMMDD')::INT
                AND (
                        u.effective_to IS NULL
                    OR src.date_key <= TO_CHAR(u.effective_to, 'YYYYMMDD')::INT
                    )
                OR
                    src.date_key < (
                        SELECT MIN(TO_CHAR(effective_from, 'YYYYMMDD')::INT)
                        FROM shopzada.dim_user
                        WHERE user_id = src.user_id
                    )
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
                    src.date_key >= TO_CHAR(m.effective_from, 'YYYYMMDD')::INT
                AND (
                        m.effective_to IS NULL
                    OR src.date_key <= TO_CHAR(m.effective_to, 'YYYYMMDD')::INT
                    )
                OR
                    src.date_key < (
                        SELECT MIN(TO_CHAR(effective_from, 'YYYYMMDD')::INT)
                        FROM shopzada.dim_merchant
                        WHERE merchant_id = src.merchant_id
                    )
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
                    src.date_key >= TO_CHAR(s.effective_from, 'YYYYMMDD')::INT
                AND (
                        s.effective_to IS NULL
                    OR src.date_key <= TO_CHAR(s.effective_to, 'YYYYMMDD')::INT
                    )
                OR
                    src.date_key < (
                        SELECT MIN(TO_CHAR(effective_from, 'YYYYMMDD')::INT)
                        FROM shopzada.dim_staff
                        WHERE staff_id = src.staff_id
                    )
            )
            ORDER BY s.effective_from
            LIMIT 1
        ) s ON TRUE



        -- ---------------- PRODUCT DIM ----------------
        LEFT JOIN shopzada.dim_product p
            ON src.product_id = p.product_id
           AND src.product_name = p.product_name;
        """
    )

    create_fact_table >> load_fact_table
