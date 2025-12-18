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


    create_fact_table >> load_fact_table
