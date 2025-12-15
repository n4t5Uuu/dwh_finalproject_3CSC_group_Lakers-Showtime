from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="dag_build_dim_user",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimension", "user"],
) as dag:

    # -------------------------------------------------
    # 1. CREATE DIM_USER TABLE
    # -------------------------------------------------
    create_dim_user = PostgresOperator(
        task_id="create_dim_user",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS shopzada;

        CREATE TABLE IF NOT EXISTS shopzada.dim_user (
            user_key SERIAL PRIMARY KEY,
            user_id VARCHAR(30) NOT NULL,
            name VARCHAR(200),
            gender VARCHAR(20),
            birth_date_key INT,
            creation_date_key INT,
            street VARCHAR(200),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            user_type VARCHAR(50),
            device_address VARCHAR(50),
            job_title VARCHAR(200),
            job_level VARCHAR(200),
            credit_card_number VARCHAR(40),
            issuing_bank VARCHAR(50),
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        );
        """
    )

    # -------------------------------------------------
    # 2. LOAD DIM_USER WITH SCD TYPE 2 LOGIC
    # -------------------------------------------------
    load_dim_user = PostgresOperator(
        task_id="load_dim_user",
        postgres_conn_id="postgres_default",
        sql="""
        TRUNCATE shopzada.dim_user;

        WITH ordered_users AS (
            SELECT
                u.*,
                LEAD(u.creation_date) OVER (
                    PARTITION BY u.user_id
                    ORDER BY u.creation_date
                ) AS next_creation_date
            FROM staging.user_data_all u
        )
        INSERT INTO shopzada.dim_user (
            user_id,
            name,
            gender,
            birth_date_key,
            creation_date_key,
            street,
            city,
            state,
            country,
            user_type,
            device_address,
            job_title,
            job_level,
            credit_card_number,
            issuing_bank,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            user_id,
            name,
            gender,
            TO_CHAR(birthdate, 'YYYYMMDD')::INT AS birth_date_key,
            TO_CHAR(creation_date, 'YYYYMMDD')::INT AS creation_date_key,
            street,
            city,
            state,
            country,
            user_type,
            device_address,
            job_title,
            job_level,
            credit_card_number,
            issuing_bank,
            DATE(creation_date) AS effective_from,
            CASE
                WHEN next_creation_date IS NOT NULL
                    THEN DATE(next_creation_date - INTERVAL '1 day')
                ELSE NULL
            END AS effective_to,
            CASE
                WHEN next_creation_date IS NULL THEN TRUE
                ELSE FALSE
            END AS is_current
        FROM ordered_users;


        -- =====================================================
        -- INSERT UNKNOWN USER (SCD2 SAFETY ROW)
        -- =====================================================

        INSERT INTO shopzada.dim_user (
            user_key,
            user_id,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            0              AS user_key,
            'UNKNOWN'      AS user_id,
            DATE '1900-01-01' AS effective_from,
            DATE '9999-12-31' AS effective_to,
            FALSE          AS is_current
        WHERE NOT EXISTS (
            SELECT 1
            FROM shopzada.dim_user
            WHERE user_key = 0
        );

        """
    )

    # -------------------------------------------------
    # DAG ORDER
    # -------------------------------------------------
    create_dim_user >> load_dim_user
