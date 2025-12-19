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


-- Remove this when doing test cases
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