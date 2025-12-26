import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# ---------------------------------
# Load environment variables
# ---------------------------------
load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)

# ---------------------------------
# DATE DIMENSION (FK-SAFE)
# ---------------------------------
def build_dim_date(conn):
    dates = pd.date_range("2024-01-01", "2024-12-31")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["quarter"] = df["full_date"].dt.quarter
    df["month"] = df["full_date"].dt.month
    df["day"] = df["full_date"].dt.day
    df["month_name"] = df["full_date"].dt.strftime("%B")
    df["day_name"] = df["full_date"].dt.strftime("%A")
    df["week_of_year"] = df["full_date"].dt.isocalendar().week
    df["is_weekend"] = df["day_name"].isin(["Saturday", "Sunday"])
    df["is_holiday"] = False

    # FK-SAFE: use DELETE instead of TRUNCATE
    conn.execute(text("DELETE FROM warehouse.dim_date"))

    df.to_sql(
        "dim_date",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False
    )

# ---------------------------------
# PAYMENT METHOD DIMENSION
# ---------------------------------
def load_payment_methods(conn):
    methods = [
        ("Credit Card", "Online"),
        ("Debit Card", "Online"),
        ("UPI", "Online"),
        ("Net Banking", "Online"),
        ("Cash on Delivery", "Offline")
    ]

    conn.execute(text("DELETE FROM warehouse.dim_payment_method"))

    conn.execute(
        text("""
            INSERT INTO warehouse.dim_payment_method
            (payment_method_name, payment_type)
            VALUES (:name, :type)
        """),
        [{"name": m[0], "type": m[1]} for m in methods]
    )

# ---------------------------------
# CUSTOMER DIMENSION (SCD TYPE 2)
# ---------------------------------
def load_dim_customers(conn):
    customers = pd.read_sql("SELECT * FROM production.customers", conn)

    # Expire existing records
    conn.execute(text("""
        UPDATE warehouse.dim_customers
        SET is_current = FALSE,
            end_date = CURRENT_DATE
        WHERE is_current = TRUE
    """))

    customers["full_name"] = customers["first_name"] + " " + customers["last_name"]
    customers["customer_segment"] = "New"
    customers["effective_date"] = datetime.today().date()
    customers["end_date"] = None
    customers["is_current"] = True

    customers[[
        "customer_id", "full_name", "email", "city", "state",
        "country", "age_group", "customer_segment",
        "registration_date", "effective_date", "end_date", "is_current"
    ]].to_sql(
        "dim_customers",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False
    )

# ---------------------------------
# PRODUCT DIMENSION (SCD TYPE 2)
# ---------------------------------
def load_dim_products(conn):
    products = pd.read_sql("SELECT * FROM production.products", conn)

    conn.execute(text("""
        UPDATE warehouse.dim_products
        SET is_current = FALSE,
            end_date = CURRENT_DATE
        WHERE is_current = TRUE
    """))

    products["price_range"] = products["price"].apply(
        lambda x: "Budget" if x < 50 else "Mid-range" if x < 200 else "Premium"
    )

    products["effective_date"] = datetime.today().date()
    products["end_date"] = None
    products["is_current"] = True

    products[[
        "product_id", "product_name", "category",
        "sub_category", "brand", "price_range",
        "effective_date", "end_date", "is_current"
    ]].to_sql(
        "dim_products",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False
    )

# ---------------------------------
# FACT SALES
# ---------------------------------
def load_fact_sales(conn):
    sql = """
    INSERT INTO warehouse.fact_sales
    (
        date_key,
        customer_key,
        product_key,
        payment_method_key,
        transaction_id,
        quantity,
        unit_price,
        discount_amount,
        line_total,
        profit
    )
    SELECT
        d.date_key,
        dc.customer_key,
        dp.product_key,
        pm.payment_method_key,
        ti.transaction_id,
        ti.quantity,
        ti.unit_price,
        ti.unit_price * ti.quantity * (ti.discount_percentage / 100),
        ti.line_total,
        ti.line_total - (p.cost * ti.quantity)
    FROM production.transaction_items ti
    JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
    JOIN production.products p
        ON ti.product_id = p.product_id
    JOIN warehouse.dim_customers dc
        ON t.customer_id = dc.customer_id
       AND dc.is_current = TRUE
    JOIN warehouse.dim_products dp
        ON p.product_id = dp.product_id
       AND dp.is_current = TRUE
    JOIN warehouse.dim_payment_method pm
        ON t.payment_method = pm.payment_method_name
    JOIN warehouse.dim_date d
        ON d.full_date = t.transaction_date;
    """

    conn.execute(text(sql))

# ---------------------------------
# AGGREGATES
# ---------------------------------
def build_aggregates(conn):
    conn.execute(text("DELETE FROM warehouse.agg_daily_sales"))
    conn.execute(text("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key
    """))

# ---------------------------------
# MAIN LOAD FUNCTION
# ---------------------------------
def load_warehouse():
    print("🚀 Loading warehouse...")

    with engine.begin() as conn:

        # Clean fact & aggregates first (FK safe)
        conn.execute(text("DELETE FROM warehouse.agg_customer_metrics"))
        conn.execute(text("DELETE FROM warehouse.agg_product_performance"))
        conn.execute(text("DELETE FROM warehouse.agg_daily_sales"))
        conn.execute(text("DELETE FROM warehouse.fact_sales"))

        # Load dimensions
        build_dim_date(conn)
        load_payment_methods(conn)
        load_dim_customers(conn)
        load_dim_products(conn)

        # Load fact & aggregates
        load_fact_sales(conn)
        build_aggregates(conn)

    print("🎉 Warehouse load completed successfully")

# ---------------------------------
# ENTRY POINT
# ---------------------------------
if __name__ == "__main__":
    load_warehouse()