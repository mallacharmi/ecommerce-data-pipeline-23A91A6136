-- =========================================================
-- QUERY 1: Top 10 Products by Revenue
-- =========================================================
-- Objective: Identify best-selling products by total revenue

SELECT
    p.product_name,
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    AVG(f.unit_price) AS avg_price
FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY
    p.product_name,
    p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- =========================================================
-- QUERY 2: Monthly Sales Trend
-- =========================================================
-- Objective: Analyze revenue trends over time

SELECT
    CONCAT(d.year, '-', LPAD(d.month::TEXT, 2, '0')) AS year_month,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    AVG(f.line_total) AS average_order_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- =========================================================
-- QUERY 3: Customer Segmentation Analysis
-- =========================================================
-- Objective: Segment customers by total spend

WITH
    customer_totals AS (
        SELECT customer_key, SUM(line_total) AS total_spent
        FROM warehouse.fact_sales
        GROUP BY
            customer_key
    )
SELECT
    CASE
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(total_spent) AS avg_transaction_value
FROM customer_totals
GROUP BY
    spending_segment
ORDER BY total_revenue DESC;

-- =========================================================
-- QUERY 4: Category Performance
-- =========================================================
-- Objective: Compare revenue and profit across categories

SELECT
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.profit) AS total_profit,
    ROUND(
        (
            SUM(f.profit) / SUM(f.line_total)
        ) * 100,
        2
    ) AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY
    p.category
ORDER BY total_revenue DESC;

-- =========================================================
-- QUERY 5: Payment Method Distribution
-- =========================================================
-- Objective: Understand payment preferences

SELECT
    pm.payment_method_name AS payment_method,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    SUM(f.line_total) AS total_revenue,
    ROUND(
        COUNT(DISTINCT f.transaction_id) * 100.0 / SUM(
            COUNT(DISTINCT f.transaction_id)
        ) OVER (),
        2
    ) AS pct_of_transactions,
    ROUND(
        SUM(f.line_total) * 100.0 / SUM(SUM(f.line_total)) OVER (),
        2
    ) AS pct_of_revenue
FROM warehouse.fact_sales f
    JOIN warehouse.dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
GROUP BY
    pm.payment_method_name;

-- =========================================================
-- QUERY 6: Geographic Analysis
-- =========================================================
-- Objective: Identify high-revenue locations

SELECT
    c.state,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    ROUND(
        SUM(f.line_total) / COUNT(DISTINCT c.customer_key),
        2
    ) AS avg_revenue_per_customer
FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
GROUP BY
    c.state
ORDER BY total_revenue DESC;

-- =========================================================
-- QUERY 7: Customer Lifetime Value (CLV)
-- =========================================================
-- Objective: Measure customer value and tenure

SELECT
    c.customer_id,
    c.full_name,
    SUM(f.line_total) AS total_spent,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    CURRENT_DATE - c.registration_date AS days_since_registration,
    ROUND(AVG(f.line_total), 2) AS avg_order_value
FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
WHERE
    c.is_current = TRUE
GROUP BY
    c.customer_id,
    c.full_name,
    c.registration_date;

-- =========================================================
-- QUERY 8: Product Profitability Analysis
-- =========================================================
-- Objective: Identify most profitable products

SELECT
    p.product_name,
    p.category,
    SUM(f.profit) AS total_profit,
    ROUND(
        SUM(f.profit) / SUM(f.line_total) * 100,
        2
    ) AS profit_margin,
    SUM(f.line_total) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY
    p.product_name,
    p.category
ORDER BY total_profit DESC;

-- =========================================================
-- QUERY 9: Day of Week Sales Pattern
-- =========================================================
-- Objective: Analyze weekday vs weekend performance

SELECT
    d.day_name,
    ROUND(AVG(f.line_total), 2) AS avg_daily_revenue,
    COUNT(DISTINCT f.transaction_id) AS avg_daily_transactions,
    SUM(f.line_total) AS total_revenue
FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY
    d.day_name
ORDER BY total_revenue DESC;

-- =========================================================
-- QUERY 10: Discount Impact Analysis
-- =========================================================
-- Objective: Analyze discount effectiveness

SELECT
    CASE
        WHEN f.discount_amount = 0 THEN '0%'
        WHEN f.discount_amount / (f.unit_price * f.quantity) <= 0.10 THEN '1-10%'
        WHEN f.discount_amount / (f.unit_price * f.quantity) <= 0.25 THEN '11-25%'
        WHEN f.discount_amount / (f.unit_price * f.quantity) <= 0.50 THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    ROUND(AVG(f.discount_amount), 2) AS avg_discount_pct,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.line_total) AS total_revenue,
    ROUND(AVG(f.line_total), 2) AS avg_line_total
FROM warehouse.fact_sales f
GROUP BY
    discount_range
ORDER BY total_revenue DESC;