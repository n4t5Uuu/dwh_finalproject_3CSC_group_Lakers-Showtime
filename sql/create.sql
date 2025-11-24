-- Optional: create a dedicated schema
CREATE SCHEMA IF NOT EXISTS shopzada;
SET search_path = shopzada, public;

-- -----------------------
-- Date dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimDate (
    date_key            BIGINT PRIMARY KEY,      -- convention: YYYYMMDD e.g. 20250131
    date_full           DATE NOT NULL,
    date_day            INT NOT NULL,
    date_day_name       VARCHAR(20),
    date_month          INT NOT NULL,
    date_month_name     VARCHAR(20),
    date_quarter        INT,
    date_quarter_name   VARCHAR(20),
    date_half_year      INT,
    date_half_year_name VARCHAR(20),
    date_year           INT NOT NULL,
    date_is_weekend     BOOLEAN DEFAULT FALSE,
    date_is_holiday     BOOLEAN DEFAULT FALSE
);

-- -----------------------
-- Campaign dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
    campaign_key          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id           VARCHAR(30) UNIQUE NOT NULL,
    campaign_name         VARCHAR(100),
    campaign_description  TEXT,
    campaign_discount     NUMERIC(6,2)        -- percent or amount (choose convention)
);

-- -----------------------
-- Merchant dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
    merchant_key               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id                VARCHAR(30) UNIQUE NOT NULL,
    merchant_name              VARCHAR(100),
    merchant_street            VARCHAR(200),
    merchant_state             VARCHAR(200),
    merchant_city              VARCHAR(100),
    merchant_country           VARCHAR(100),
    merchant_contact_number    VARCHAR(50),

    merchant_creation_date_key INT,
    CONSTRAINT fk_merchant_creation_date
        FOREIGN KEY (merchant_creation_date_key) REFERENCES shopzada.dimDate(date_key)
        ON UPDATE CASCADE ON DELETE SET NULL
);

-- -----------------------
-- Staff dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
    staff_key                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    staff_id                 VARCHAR(30) UNIQUE NOT NULL,
    staff_name               VARCHAR(100),
    staff_job_level          VARCHAR(50),
    staff_street             VARCHAR(200),
    staff_state              VARCHAR(100),
    staff_city               VARCHAR(100),
    staff_country            VARCHAR(100),
    staff_contact_number     VARCHAR(50),

    staff_creation_date_key  INT,
    CONSTRAINT fk_staff_creation_date
        FOREIGN KEY (staff_creation_date_key) REFERENCES shopzada.dimDate(date_key)
        ON UPDATE CASCADE ON DELETE SET NULL
);

-- -----------------------
-- User dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimUser (
    user_key                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id                 VARCHAR(30) UNIQUE NOT NULL,

    user_creation_date_key  INT,
    user_birth_date_key     INT,
    CONSTRAINT fk_user_creation_date FOREIGN KEY (user_creation_date_key)
        REFERENCES shopzada.dimDate(date_key) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_user_birth_date    FOREIGN KEY (user_birth_date_key)
        REFERENCES shopzada.dimDate(date_key) ON UPDATE CASCADE ON DELETE SET NULL,

    user_name               VARCHAR(100),
    user_street             VARCHAR(200),
    user_state              VARCHAR(100),
    user_city               VARCHAR(100),
    user_country            VARCHAR(100),
    user_gender             VARCHAR(20),
    user_device_address     VARCHAR(50),
    user_user_type          VARCHAR(50),
    user_issuing_bank       VARCHAR(100),
    user_credit_card        VARCHAR(32),         -- store masked or tokenized; avoid raw PANs in shopzada
    user_job_title          VARCHAR(100),
    user_job_level          VARCHAR(50)
);

-- -----------------------
-- Product dimension
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.dimProduct (
    product_key     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      VARCHAR(30) UNIQUE NOT NULL,
    product_name    VARCHAR(200),
    product_type    VARCHAR(100),
    product_price   NUMERIC(12,2)                  
);

-- -----------------------
-- Fact table
-- -----------------------
CREATE TABLE IF NOT EXISTS shopzada.factLineItem (
    transaction_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_key                   BIGINT,
    product_key                BIGINT,
    campaign_key               BIGINT,
    merchant_key               BIGINT,
    staff_key                  BIGINT,

    transaction_date_key       BIGINT,    -- references dimDate.date_key (YYYYMMDD)

    price_per_quantity         NUMERIC(20,2) CHECK (price_per_quantity >= 0),
    quantity                   BIGINT CHECK (quantity >= 0),
    estimated_arrival_in_days  BIGINT CHECK (estimated_arrival_in_days >= 0),
    delay_in_days              BIGINT CHECK (delay_in_days >= 0),
    campaign_availed           BOOLEAN DEFAULT FALSE,

    -- Foreign Keys (allow NULLs for unknown dimensional keys)
    CONSTRAINT fk_fact_user       FOREIGN KEY (user_key) REFERENCES shopzada.dimUser(user_key)
        ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_fact_product    FOREIGN KEY (product_key) REFERENCES shopzada.dimProduct(product_key)
        ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_fact_campaign   FOREIGN KEY (campaign_key) REFERENCES shopzada.dimCampaign(campaign_key)
        ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_fact_merchant   FOREIGN KEY (merchant_key) REFERENCES shopzada.dimMerchant(merchant_key)
        ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_fact_staff      FOREIGN KEY (staff_key) REFERENCES shopzada.dimStaff(staff_key)
        ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT fk_fact_txn_date   FOREIGN KEY (transaction_date_key) REFERENCES shopzada.dimDate(date_key)
        ON UPDATE CASCADE ON DELETE SET NULL
);