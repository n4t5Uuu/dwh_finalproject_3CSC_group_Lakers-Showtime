CREATE TABLE IF NOT EXISTS shopzada.dimDate (
    date_key INT PRIMARY KEY,
    date_full DATE NOT NULL,
    date_day INT,
    date_day_name VARCHAR(20),
    date_month INT,
    date_month_name VARCHAR(20),
    date_quarter INT,
    date_quarter_name VARCHAR(10),
    date_half_year INT,
    date_half_year_name VARCHAR(10),
    date_year INT,
    date_is_weekend BOOLEAN,
    date_is_holiday BOOLEAN
);
