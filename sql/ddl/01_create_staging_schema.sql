-- ===============================
-- CREATE SCHEMAS
-- ===============================
CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS production;

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ===============================
-- STAGING: CUSTOMERS
-- ===============================
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    registration_date DATE,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- STAGING: PRODUCTS
-- ===============================
CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    brand VARCHAR(100),
    stock_quantity INTEGER,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- STAGING: TRANSACTIONS
-- ===============================
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(30),
    shipping_address VARCHAR(255),
    total_amount DECIMAL(12, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- STAGING: TRANSACTION ITEMS
-- ===============================
CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    line_total DECIMAL(12, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);