CREATE TABLE IF NOT EXISTS shopzada.dimMerchant (
    merchant_key VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50) UNIQUE NOT NULL,

    name VARCHAR(200),
    street VARCHAR(200),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(50),
    contact_number VARCHAR(50),

    creation_date TIMESTAMP,
    merchant_creation_date_key INT REFERENCES shopzada.dimDate(date_key)
);
