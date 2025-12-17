CREATE SCHEMA IF NOT EXISTS shopzada;

CREATE TABLE IF NOT EXISTS shopzada.dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR(30) NOT NULL,
    campaign_name VARCHAR(255),
    campaign_description TEXT,
    discount_pct INTEGER,
    UNIQUE (campaign_id)
);

-- Remove this when doing test cases
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