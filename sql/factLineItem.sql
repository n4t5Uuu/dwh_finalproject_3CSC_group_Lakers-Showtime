-- one row per product in an order.
-- Based from clean_data/facts/factLineItem.csv
CREATE TABLE IF NOT EXISTS shopzada.factLineItem (
    line_item_key BIGSERIAL PRIMARY KEY,

    order_id VARCHAR(60) NOT NULL,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    price_per_quantity NUMERIC(12,2),
    quantity INT,
    line_amount NUMERIC(14,2),

    user_key VARCHAR(50),
    merchant_key VARCHAR(50),
    staff_key VARCHAR(50),
    date_key INT,

    FOREIGN KEY (order_id) REFERENCES shopzada.factOrders(order_id),
    FOREIGN KEY (user_key) REFERENCES shopzada.dimUser(user_key),
    FOREIGN KEY (merchant_key) REFERENCES shopzada.dimMerchant(merchant_key),
    FOREIGN KEY (staff_key) REFERENCES shopzada.dimStaff(staff_key),
    FOREIGN KEY (date_key) REFERENCES shopzada.dimDate(date_key)
);
