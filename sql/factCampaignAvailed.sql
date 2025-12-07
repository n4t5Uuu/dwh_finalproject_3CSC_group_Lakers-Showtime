
CREATE TABLE IF NOT EXISTS shopzada.factCampaignAvailed (
    campaign_availed_key BIGSERIAL PRIMARY KEY,

    order_id VARCHAR(60),
    campaign_key VARCHAR(50),

    estimated_arrival_days INT,
    transaction_date_key INT REFERENCES shopzada.dimDate(date_key),

    FOREIGN KEY (order_id) REFERENCES shopzada.factOrders(order_id),
    FOREIGN KEY (campaign_key) REFERENCES shopzada.dimCampaign(campaign_key)
);
