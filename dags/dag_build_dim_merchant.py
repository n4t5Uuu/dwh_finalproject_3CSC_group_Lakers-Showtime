from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_build_dim_merchant",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimension", "merchant", "scd2"],
) as dag:

    # -------------------------------------------------
    # 1. CREATE DIM_MERCHANT (SCD TYPE 2)
    # -------------------------------------------------
    create_dim_merchant = PostgresOperator(
        task_id="create_dim_merchant",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.dim_merchant (
            merchant_key BIGSERIAL PRIMARY KEY,
            merchant_id VARCHAR(30) NOT NULL,

            name VARCHAR(255),
            street VARCHAR(255),
            state VARCHAR(100),
            city VARCHAR(100),
            country VARCHAR(100),
            contact_number VARCHAR(50),

            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        );
        """
    )

    # -------------------------------------------------
    # 2. LOAD DIM_MERCHANT (SCD TYPE 2)
    # -------------------------------------------------
    load_dim_merchant = PostgresOperator(
        task_id="load_dim_merchant",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.dim_merchant;

        INSERT INTO shopzada.dim_merchant (
            merchant_id,
            name,
            street,
            state,
            city,
            country,
            contact_number,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            m.merchant_id,
            m.name,
            m.street,
            m.state,
            m.city,
            m.country,
            m.contact_number,

            m.creation_date::date AS effective_from,

            LEAD(m.creation_date::date) OVER (
                PARTITION BY m.merchant_id
                ORDER BY m.creation_date
            ) - INTERVAL '1 day' AS effective_to,

            CASE
                WHEN LEAD(m.creation_date) OVER (
                    PARTITION BY m.merchant_id
                    ORDER BY m.creation_date
                ) IS NULL
                THEN TRUE
                ELSE FALSE
            END AS is_current

        FROM staging.merchant_data m;


        -- =====================================================
        -- INSERT UNKNOWN USER (SCD2 SAFETY ROW)
        -- =====================================================

        INSERT INTO shopzada.dim_merchant (
            merchant_key,
            merchant_id,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            0,
            'UNKNOWN',
            DATE '1900-01-01',
            DATE '9999-12-31',
            FALSE
        WHERE NOT EXISTS (
            SELECT 1 FROM shopzada.dim_merchant WHERE merchant_key = 0
        );

        """
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    create_dim_merchant >> load_dim_merchant
