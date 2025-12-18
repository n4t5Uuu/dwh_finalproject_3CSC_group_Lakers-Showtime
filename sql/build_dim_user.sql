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

-- Remove this when doing test cases
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
-- LATE-ARRIVING USERS (FROM ORDERS)
-- =====================================================
INSERT INTO shopzada.dim_user (
    user_id,
    effective_from,
    effective_to,
    is_current
)
SELECT DISTINCT
    o.user_id,
    DATE '1960-01-01',
    CAST(NULL AS DATE),
    TRUE
FROM staging.orders_clean o
LEFT JOIN shopzada.dim_user du
    ON o.user_id = du.user_id
WHERE du.user_id IS NULL;


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