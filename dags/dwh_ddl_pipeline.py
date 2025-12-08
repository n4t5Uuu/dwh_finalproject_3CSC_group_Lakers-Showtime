from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# -----------------------------------
# FULL SQL DDL SCRIPT DIRECTLY EMBEDDED
# -----------------------------------
SQL_CREATE_ALL_TABLES = """


CREATE SCHEMA IF NOT EXISTS shopzada;

-- ========================================
-- DIM DATE
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimDate (
    date_key            INT PRIMARY KEY,
    date_full           DATE,
    date_day            INT,
    date_day_name       VARCHAR(20),
    date_month          INT,
    date_month_name     VARCHAR(20),
    date_quarter        INT,
    date_quarter_name   VARCHAR(10),
    date_half_year      INT,
    date_half_year_name VARCHAR(10),
    date_year           INT,
    date_is_weekend     BOOLEAN,
    date_is_holiday     BOOLEAN
);

-- ========================================
-- DIM USER
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimUser (
    user_key                VARCHAR(30) PRIMARY KEY,
    user_id                 VARCHAR(30) UNIQUE NOT NULL,
    name                    VARCHAR(200),
    age                     INT,
    gender                  VARCHAR(20),
    birthdate               DATE,
    user_birth_date_key     INT,
    creation_date           DATE,
    user_creation_date_key  INT,
    email                   VARCHAR(200),
    contact_number          VARCHAR(50),
    street                  VARCHAR(200),
    city                    VARCHAR(100),
    state                   VARCHAR(100),
    country                 VARCHAR(100)
);

-- ========================================
-- DIM MERCHANT
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
    merchant_key               VARCHAR(40) PRIMARY KEY,
    merchant_id                VARCHAR(40) NOT NULL,
    name                       VARCHAR(200),
    street                     VARCHAR(200),
    state                      VARCHAR(100),
    city                       VARCHAR(100),
    country                    VARCHAR(100),
    contact_number             VARCHAR(50),
    creation_date              DATE,
    merchant_creation_date_key INT
);

-- ========================================
-- DIM STAFF
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
    staff_key                VARCHAR(40) PRIMARY KEY,
    staff_id                 VARCHAR(40) UNIQUE NOT NULL,
    name                     VARCHAR(200),
    job_level                VARCHAR(50),
    street                   VARCHAR(200),
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    country                  VARCHAR(100),
    contact_number           VARCHAR(50),
    creation_date            DATE,
    staff_creation_date_key  INT
);

-- ========================================
-- DIM CAMPAIGN
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
    campaign_key        VARCHAR(40) PRIMARY KEY,
    campaign_id         VARCHAR(40) UNIQUE NOT NULL,
    campaign_name       VARCHAR(200),
    campaign_description TEXT,
    discount_pct        INT
);

-- ========================================
-- DIM PRODUCT
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.dimProduct (
    product_key        VARCHAR(40) PRIMARY KEY,
    product_id         VARCHAR(40) UNIQUE NOT NULL,
    product_name       VARCHAR(255),
    product_type       VARCHAR(100),
    price              NUMERIC(12,2)
);


-- ========================================
-- FACT ORDERS
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.factOrders (
    order_key              BIGSERIAL PRIMARY KEY,
    order_id               VARCHAR(100) UNIQUE,
    user_key               VARCHAR(30) REFERENCES shopzada.dimUser(user_key),
    merchant_key           VARCHAR(40) REFERENCES shopzada.dimMerchant(merchant_key),
    staff_key              VARCHAR(40) REFERENCES shopzada.dimStaff(staff_key),
    date_key               INT REFERENCES shopzada.dimDate(date_key),
    transaction_amount     NUMERIC(12,2),
    estimated_arrival_in_days INT,
    delay_in_days          INT
);

-- ========================================
-- FACT LINE ITEM
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.factLineItem (
    line_key            BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(100),
    user_key            VARCHAR(30),
    merchant_key        VARCHAR(40),
    staff_key           VARCHAR(40),
    product_id          VARCHAR(50),
    price_per_quantity  NUMERIC(12,2),
    quantity            INT,
    line_amount         NUMERIC(14,2),
    date_key            INT
);

-- ========================================
-- FACT CAMPAIGN AVAILED
-- ========================================
CREATE TABLE IF NOT EXISTS shopzada.factCampaignAvailed (
    campaign_availed_key  BIGSERIAL PRIMARY KEY,

    -- Natural grain
    order_id              VARCHAR(100),
    campaign_key          VARCHAR(40) REFERENCES shopzada.dimCampaign(campaign_key),

    -- Foreign keys for filtering capability
    user_key              VARCHAR(30),
    merchant_key          VARCHAR(40),
    staff_key             VARCHAR(40),

    -- Date of the event
    date_key              INT REFERENCES shopzada.dimDate(date_key),

    -- Measures
    availed               INT,
    discount_pct          INT
);

"""

# -----------------------------------
# DAG DEFINITION
# -----------------------------------
with DAG(
    dag_id="dwh_ddl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ddl", "dwh"],
) as dag:

    create_all_tables = PostgresOperator(
        task_id="create_all_tables",
        postgres_conn_id="postgres_default",
        sql=SQL_CREATE_ALL_TABLES,
    )
