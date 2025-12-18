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


-- Remove this when doing test cases
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