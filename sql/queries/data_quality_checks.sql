-- ===============================
-- COMPLETENESS CHECKS
-- ===============================

-- NULLs in mandatory customer fields
SELECT COUNT(*) AS null_count
FROM production.customers
WHERE
    customer_id IS NULL
    OR email IS NULL;

-- Transactions without items
SELECT COUNT(*) AS transactions_without_items
FROM production.transactions t
    LEFT JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
WHERE
    ti.transaction_id IS NULL;

-- ===============================
-- UNIQUENESS CHECKS
-- ===============================

-- Duplicate customer emails
SELECT email, COUNT(*)
FROM production.customers
GROUP BY
    email
HAVING
    COUNT(*) > 1;

-- Duplicate transactions (same customer, time, amount)
SELECT
    customer_id,
    transaction_date,
    transaction_time,
    total_amount,
    COUNT(*)
FROM production.transactions
GROUP BY
    customer_id,
    transaction_date,
    transaction_time,
    total_amount
HAVING
    COUNT(*) > 1;

-- ===============================
-- REFERENTIAL INTEGRITY
-- ===============================

-- Orphan transactions (customer missing)
SELECT COUNT(*)
FROM production.transactions t
    LEFT JOIN production.customers c ON t.customer_id = c.customer_id
WHERE
    c.customer_id IS NULL;

-- Orphan transaction items (transaction missing)
SELECT COUNT(*)
FROM production.transaction_items ti
    LEFT JOIN production.transactions t ON ti.transaction_id = t.transaction_id
WHERE
    t.transaction_id IS NULL;

-- Orphan transaction items (product missing)
SELECT COUNT(*)
FROM production.transaction_items ti
    LEFT JOIN production.products p ON ti.product_id = p.product_id
WHERE
    p.product_id IS NULL;

-- ===============================
-- RANGE & BUSINESS RULE CHECKS
-- ===============================

-- Invalid price or cost
SELECT COUNT(*)
FROM production.products
WHERE
    price <= 0
    OR cost <= 0
    OR cost >= price;

-- Invalid quantity or discount
SELECT COUNT(*)
FROM production.transaction_items
WHERE
    quantity <= 0
    OR discount_percentage < 0
    OR discount_percentage > 100;

-- ===============================
-- CONSISTENCY CHECKS
-- ===============================

-- Line total mismatch
SELECT COUNT(*)
FROM production.transaction_items
WHERE
    ROUND(
        quantity * unit_price * (1 - discount_percentage / 100),
        2
    ) != line_total;

-- Transaction total mismatch
SELECT COUNT(*)
FROM production.transactions t
    JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY
    t.transaction_id,
    t.total_amount
HAVING
    ROUND(SUM(ti.line_total), 2) != t.total_amount;

-- ===============================
-- ACCURACY CHECKS
-- ===============================

-- Future transaction dates
SELECT COUNT(*)
FROM production.transactions
WHERE
    transaction_date > CURRENT_DATE;

-- Registration after transaction
SELECT COUNT(*)
FROM production.transactions t
    JOIN production.customers c ON t.customer_id = c.customer_id
WHERE
    c.registration_date > t.transaction_date;