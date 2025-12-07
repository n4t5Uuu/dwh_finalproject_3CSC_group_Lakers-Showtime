CREATE TABLE IF NOT EXISTS shopzada.dimStaff (
    staff_key VARCHAR(50) PRIMARY KEY,
    staff_id VARCHAR(50) UNIQUE NOT NULL,

    name VARCHAR(200),
    job_level VARCHAR(100),
    street VARCHAR(200),
    state VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(50),
    contact_number VARCHAR(50),

    creation_date TIMESTAMP,
    staff_creation_date_key INT REFERENCES shopzada.dimDate(date_key)
);
