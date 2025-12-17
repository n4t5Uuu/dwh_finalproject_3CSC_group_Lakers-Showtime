from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="dag_build_all_dim",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "dimensions", "kimball"],
) as dag:

    build_dim_campaign = PostgresOperator(
        task_id="build_dim_campaign",
        postgres_conn_id="postgres_default",
        sql=""" 
            CREATE SCHEMA IF NOT EXISTS shopzada;

            CREATE TABLE IF NOT EXISTS shopzada.dim_campaign (
                campaign_key SERIAL PRIMARY KEY,
                campaign_id VARCHAR(30) NOT NULL,
                campaign_name VARCHAR(255),
                campaign_description TEXT,
                discount_pct INTEGER,
                UNIQUE (campaign_id)
            );

            -- ============================================
            -- SNAPSHOT-BASED SCD TYPE 2 REBUILD
            -- ============================================
            TRUNCATE shopzada.dim_campaign;

            INSERT INTO shopzada.dim_campaign (
                campaign_id,
                campaign_name,
                campaign_description,
                discount_pct
            )
            SELECT
                c.campaign_id,
                c.campaign_name,
                c.campaign_description,
                c.discount_pct
            FROM staging.campaign_data c;
        """
    )

    build_dim_date = PostgresOperator(
        task_id="build_dim_date",
        postgres_conn_id="postgres_default",
        sql=""" 
            CREATE SCHEMA IF NOT EXISTS shopzada;
            CREATE TABLE IF NOT EXISTS shopzada.dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE NOT NULL,
                day INT,
                day_name VARCHAR(10),
                month INT,
                month_name VARCHAR(10),
                quarter INT,
                quarter_name VARCHAR(6),
                year INT,
                is_weekend BOOLEAN
            );

            TRUNCATE shopzada.dim_date;

            INSERT INTO shopzada.dim_date
            SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT        AS date_key,
                d                                 AS full_date,
                EXTRACT(DAY FROM d)::INT          AS day,
                TRIM(TO_CHAR(d, 'Day'))           AS day_name,
                EXTRACT(MONTH FROM d)::INT        AS month,
                TRIM(TO_CHAR(d, 'Month'))         AS month_name,
                EXTRACT(QUARTER FROM d)::INT      AS quarter,
                'Q' || EXTRACT(QUARTER FROM d)    AS quarter_name,
                EXTRACT(YEAR FROM d)::INT         AS year,
                CASE
                    WHEN EXTRACT(ISODOW FROM d) IN (6, 7)
                    THEN TRUE
                    ELSE FALSE
                END                               AS is_weekend
            FROM generate_series(
                DATE '1970-01-01',
                DATE '2030-12-31',
                INTERVAL '1 day'
            ) AS d;

 """
    )

    build_dim_merchant = PostgresOperator(
        task_id="build_dim_merchant",
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

            -- ============================================
            -- SNAPSHOT-BASED SCD TYPE 2 REBUILD
            -- ============================================

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

            -- ============================================
            -- UNKNOWN MEMBER (SURROGATE KEY = 0)
            -- ============================================

            INSERT INTO shopzada.dim_merchant (
                merchant_key,
                merchant_id,
                effective_from,
                effective_to,
                is_current
            )
            VALUES (
                0,
                'UNKNOWN',
                DATE '1900-01-01',
                DATE '9999-12-31',
                FALSE
            )
            ON CONFLICT DO NOTHING;
        """ 
    )

    build_dim_user = PostgresOperator(
        task_id="build_dim_user",
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

            -- =====================================================
            -- SNAPSHOT-BASED SCD TYPE 2 (DEV / TEST PHASE)
            -- =====================================================
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
                creation_date::date AS effective_from,
                CASE
                    WHEN next_creation_date IS NOT NULL
                        THEN (next_creation_date::date - INTERVAL '1 day')
                    ELSE NULL
                END AS effective_to,
                CASE
                    WHEN next_creation_date IS NULL THEN TRUE
                    ELSE FALSE
                END AS is_current
            FROM ordered_users;

            -- =====================================================
            -- UNKNOWN USER (SURROGATE KEY = 0)
            -- =====================================================
            INSERT INTO shopzada.dim_user (
                user_key,
                user_id,
                effective_from,
                effective_to,
                is_current
            )
            VALUES (
                0,
                'UNKNOWN',
                DATE '1900-01-01',
                DATE '9999-12-31',
                FALSE
            )
            ON CONFLICT DO NOTHING;
            """
    )

    build_dim_product = PostgresOperator(
        task_id="build_dim_product",
        postgres_conn_id="postgres_default",
        sql=""" 
            CREATE SCHEMA IF NOT EXISTS shopzada;

            CREATE TABLE IF NOT EXISTS shopzada.dim_product (
                product_key SERIAL PRIMARY KEY,
                product_id VARCHAR(30) NOT NULL,
                product_name VARCHAR(255),
                product_type VARCHAR(100),
                price NUMERIC(10,2),
                UNIQUE (product_id, product_name)
            );

            -- ============================================
            -- TYPE 1 DIMENSION (DEV / TEST PHASE)
            -- ============================================
            TRUNCATE shopzada.dim_product;

            INSERT INTO shopzada.dim_product (
                product_id,
                product_name,
                product_type,
                price
            )
            SELECT
                p.product_id,
                p.product_name,
                p.product_type,
                p.price
            FROM staging.product_list p;
            """
    )

    build_dim_staff = PostgresOperator(
        task_id="build_dim_staff",
        postgres_conn_id="postgres_default",
        sql=""" 
            CREATE SCHEMA IF NOT EXISTS shopzada;

            CREATE TABLE IF NOT EXISTS shopzada.dim_staff (
                staff_key BIGSERIAL PRIMARY KEY,
                staff_id VARCHAR(30) NOT NULL,

                name VARCHAR(255),
                job_level VARCHAR(50),
                street VARCHAR(255),
                state VARCHAR(100),
                city VARCHAR(100),
                country VARCHAR(100),
                contact_number VARCHAR(50),

                effective_from DATE NOT NULL,
                effective_to DATE,
                is_current BOOLEAN NOT NULL
            );

            -- ============================================
            -- SNAPSHOT-BASED SCD TYPE 2 (DEV / TEST PHASE)
            -- ============================================
            TRUNCATE shopzada.dim_staff;

            INSERT INTO shopzada.dim_staff (
                staff_id,
                name,
                job_level,
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
                s.staff_id,
                s.name,
                s.job_level,
                s.street,
                s.state,
                s.city,
                s.country,
                s.contact_number,

                s.creation_date::date AS effective_from,

                LEAD(s.creation_date::date) OVER (
                    PARTITION BY s.staff_id
                    ORDER BY s.creation_date
                ) - INTERVAL '1 day' AS effective_to,

                CASE
                    WHEN LEAD(s.creation_date) OVER (
                        PARTITION BY s.staff_id
                        ORDER BY s.creation_date
                    ) IS NULL
                    THEN TRUE
                    ELSE FALSE
                END AS is_current

            FROM staging.staff_data s;


            INSERT INTO shopzada.dim_staff (
                staff_key,
                staff_id,
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
                SELECT 1 FROM shopzada.dim_staff WHERE staff_key = 0
            );
            """
    )


    # parallel by default
    [build_dim_campaign, build_dim_date, build_dim_merchant, build_dim_user, build_dim_product, build_dim_staff]