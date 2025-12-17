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
    TO_CHAR(d, 'YYYYMMDD')::INT       AS date_key,
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