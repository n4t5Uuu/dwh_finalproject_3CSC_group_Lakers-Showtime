CREATE TABLE IF NOT EXISTS shopzada.dimUser (
    user_key VARCHAR(30) PRIMARY KEY,
    user_id VARCHAR(30) UNIQUE NOT NULL,
    name VARCHAR(200),
    street VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(50),
    gender VARCHAR(20),
    device_address VARCHAR(200),
    user_type VARCHAR(50),
    creation_date_key INT REFERENCES shopzada.dimDate(date_key),
    birth_date_key INT REFERENCES shopzada.dimDate(date_key)
);
