CREATE SCHEMA IF NOT EXISTS staging;

-- =========================
-- CUSTOMER MANAGEMENT
-- =========================
CREATE TABLE IF NOT EXISTS staging.user_data_all (
    user_id VARCHAR(30),
    name VARCHAR(200),
    creation_date TIMESTAMP,
    birthdate TIMESTAMP,
    gender VARCHAR(20),
    street VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    device_address VARCHAR(50),
    user_type VARCHAR(50),
    job_title VARCHAR(200),
    job_level VARCHAR(200),
    credit_card_number VARCHAR(40),
    issuing_bank VARCHAR(50)
);

-- =========================
-- OPERATIONS
-- =========================
CREATE TABLE IF NOT EXISTS staging.orders_clean (
    order_id VARCHAR(100),
    user_id VARCHAR(30),
    transaction_date DATE,
    date_key INT,
    estimated_arrival_days INT
);

CREATE TABLE IF NOT EXISTS staging.order_delays_clean (
    order_id VARCHAR(100),
    delay_in_days INT
);

-- =========================
-- BUSINESS 
-- =========================
CREATE TABLE IF NOT EXISTS staging.product_list (
    product_id VARCHAR(30),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price NUMERIC(10,2)
);

-- =========================
-- MARKETING
-- =========================
CREATE TABLE IF NOT EXISTS staging.transactional_campaign_clean (
    order_id VARCHAR(100),
    campaign_id VARCHAR(40),
    transaction_date DATE,
    date_key INT,
    estimated_arrival_days INT,
    availed INT
);

CREATE TABLE IF NOT EXISTS staging.campaign_data (
    campaign_id VARCHAR(30),
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount_pct INTEGER
);

-- =========================
-- ENTERPRISE
-- =========================
CREATE TABLE IF NOT EXISTS staging.staff_data (
    staff_id VARCHAR(30),
    name VARCHAR(255),
    job_level VARCHAR(50),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    staff_creation_date_key INTEGER
);

CREATE TABLE IF NOT EXISTS staging.merchant_data (
    merchant_id VARCHAR(30),
    name VARCHAR(255),
    street VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(50),
    creation_date TIMESTAMP,
    merchant_creation_date_key INTEGER
);

CREATE TABLE IF NOT EXISTS staging.order_with_merchant_clean (
    order_id VARCHAR(100),
    merchant_id VARCHAR(40),
    staff_id VARCHAR(40)
);

-- =========================
-- FACT SOURCE 
-- =========================
CREATE TABLE IF NOT EXISTS staging.fact_line_item_src (
    order_id     VARCHAR(100),
    product_id   VARCHAR(50),
    product_name VARCHAR(255),

    user_id      VARCHAR(30),
    merchant_id  VARCHAR(30),
    staff_id     VARCHAR(30),

    unit_price   NUMERIC(12,2),
    quantity     INT,
    line_amount  NUMERIC(14,2),

    date_key     INT,
    campaign_id  VARCHAR(40)
);

