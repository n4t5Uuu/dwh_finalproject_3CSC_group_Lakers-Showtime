CREATE SCHEMA IF NOT EXISTS shopzada;

CREATE TABLE IF NOT EXISTS shopzada.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(30) NOT NULL,
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    price NUMERIC(10,2),
    UNIQUE (product_id, product_name)
);


-- Remove this when doing test cases
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

INSERT INTO shopzada.dim_product (
    product_id,
    product_name,
    product_type,
    price
)
SELECT DISTINCT
    f.product_id,
    COALESCE(f.product_name, 'UNKNOWN'),
    'UNKNOWN',
    0.00
FROM staging.fact_line_item_src f
LEFT JOIN shopzada.dim_product dp
    ON f.product_id = dp.product_id
WHERE dp.product_id IS NULL;