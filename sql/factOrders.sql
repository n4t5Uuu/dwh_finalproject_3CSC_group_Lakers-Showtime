-- Based from clean_data/operations/orders_final_ready.csv
CREATE TABLE IF NOT EXISTS shopzada.factOrders (
    order_id VARCHAR(60) PRIMARY KEY,

    user_key VARCHAR(50),
    merchant_key VARCHAR(50),
    staff_key VARCHAR(50),

    estimated_arrival_in_days INT,
    delay_in_days INT,

    transaction_date_key INT REFERENCES shopzada.dimDate(date_key),

    FOREIGN KEY (user_key) REFERENCES shopzada.dimUser(user_key),
    FOREIGN KEY (merchant_key) REFERENCES shopzada.dimMerchant(merchant_key),
    FOREIGN KEY (staff_key) REFERENCES shopzada.dimStaff(staff_key)
);
