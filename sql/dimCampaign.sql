CREATE TABLE IF NOT EXISTS shopzada.dimCampaign (
    campaign_key VARCHAR(40) PRIMARY KEY,
    campaign_id VARCHAR(40) UNIQUE NOT NULL,
    campaign_name VARCHAR(200),
    campaign_description TEXT,
    discount_pct INT
);
