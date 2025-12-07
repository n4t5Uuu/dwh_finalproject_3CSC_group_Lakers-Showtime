CREATE SCHEMA IF NOT EXISTS shopzada;

CREATE TABLE IF NOT EXISTS shopzada.dimDate (
    date_key INT PRIMARY KEY,
    date_full DATE NOT NULL,
    date_day INT,
    date_day_name VARCHAR(20),
    date_month INT,
    date_month_name VARCHAR(20),
    date_quarter INT,
    date_quarter_name VARCHAR(10),
    date_half_year INT,
    date_half_year_name VARCHAR(10),
    date_year INT,
    date_is_weekend BOOLEAN,
    date_is_holiday BOOLEAN
);


CREATE TABLE IF NOT EXISTS shopzada.dimUser (
    user_key VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    name VARCHAR(200),
    street VARCHAR(200),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    gender VARCHAR(20),
    device_address VARCHAR(200),
    user_type VARCHAR(50),
    user_creation_date_key INT REFERENCES shopzada.dimDate(date_key),
    user_birth_date_key INT REFERENCES shopzada.dimDate(date_key)
);

CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
    merchant_key VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50),
    creation_date TIMESTAMP,
    name VARCHAR(200),
    street VARCHAR(200),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(30),
    merchant_creation_date_key INT REFERENCES shopzada.dimDate(date_key)
);

CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
    staff_key VARCHAR(50) PRIMARY KEY,
    staff_id VARCHAR(50),
    name VARCHAR(200),
    job_level VARCHAR(50),
    street VARCHAR(200),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    contact_number VARCHAR(30),
    creation_date TIMESTAMP,
    staff_creation_date_key INT REFERENCES shopzada.dimDate(date_key)
);

CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
    campaign_key VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(50) UNIQUE NOT NULL,
    campaign_name VARCHAR(200),
    campaign_description TEXT,
    discount_pct INT
);

-- Fact Tables --

CREATE TABLE IF NOT EXISTS shopzada.factOrders (
    order_key BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    user_key VARCHAR(50) REFERENCES shopzada.dimUser(user_key),
    merchant_key VARCHAR(50) REFERENCES shopzada.dimMerchant(merchant_key),
    staff_key VARCHAR(50) REFERENCES shopzada.dimStaff(staff_key),

    price NUMERIC(12,2),
    transaction_date_key INT REFERENCES shopzada.dimDate(date_key),
    estimated_arrival_in_days INT,
    delay_in_days INT,

    UNIQUE(order_id, user_key)
);

CREATE TABLE IF NOT EXISTS shopzada.factLineItem (
    line_item_key BIGSERIAL PRIMARY KEY,

    order_id VARCHAR(50),
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    price_per_quantity NUMERIC(12,2),
    quantity INT,

    user_key VARCHAR(50) REFERENCES shopzada.dimUser(user_key),
    merchant_key VARCHAR(50) REFERENCES shopzada.dimMerchant(merchant_key),
    staff_key VARCHAR(50) REFERENCES shopzada.dimStaff(staff_key),
    date_key INT REFERENCES shopzada.dimDate(date_key)
);

CREATE TABLE IF NOT EXISTS shopzada.factCampaignAvailed (
    campaign_availed_key BIGSERIAL PRIMARY KEY,

    order_id VARCHAR(50),
    user_key VARCHAR(50) REFERENCES shopzada.dimUser(user_key),
    campaign_key VARCHAR(50) REFERENCES shopzada.dimCampaign(campaign_key),
    date_key INT REFERENCES shopzada.dimDate(date_key),

    transaction_date_key INT REFERENCES shopzada.dimDate(date_key),
    estimated_arrival_in_days INT,
    delay_in_days INT
);
