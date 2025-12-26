-- =========================================
-- MONITORING QUERIES
-- =========================================

-- 1️⃣ DATA FRESHNESS CHECK
-- Latest timestamps from each layer
SELECT 'staging' AS layer, MAX(loaded_at) AS latest_timestamp
FROM staging.customers
UNION ALL
SELECT 'production', MAX(created_at)
FROM production.transactions
UNION ALL
SELECT 'warehouse', MAX(created_at)
FROM warehouse.fact_sales;

-- 2️⃣ DAILY TRANSACTION VOLUME (LAST 30 DAYS)
SELECT d.full_date AS date, COUNT(DISTINCT f.transaction_id) AS daily_transactions
FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
WHERE
    d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    d.full_date
ORDER BY d.full_date;

-- 3️⃣ DATA QUALITY CHECKS

-- Orphan transactions
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
    LEFT JOIN production.customers c ON t.customer_id = c.customer_id
WHERE
    c.customer_id IS NULL;

-- Orphan transaction items
SELECT COUNT(*) AS orphan_items
FROM production.transaction_items ti
    LEFT JOIN production.transactions t ON ti.transaction_id = t.transaction_id
WHERE
    t.transaction_id IS NULL;

-- Null violations (mandatory fields example)
SELECT COUNT(*) AS null_email_count
FROM production.customers
WHERE
    email IS NULL;

-- 4️⃣ DATABASE CONNECTION HEALTH
SELECT COUNT(*) AS active_connections FROM pg_stat_activity;

-- 5️⃣ DATABASE SIZE
SELECT
    pg_database_size (current_database ()) / 1024 / 1024 AS db_size_mb;