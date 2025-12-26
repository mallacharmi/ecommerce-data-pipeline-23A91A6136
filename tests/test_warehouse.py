from sqlalchemy import create_engine, inspect
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

inspector = inspect(engine)

def test_warehouse_tables_exist():
    tables = inspector.get_table_names(schema="warehouse")
    required = [
        "dim_customers",
        "dim_products",
        "dim_date",
        "dim_payment_method",
        "fact_sales",
        "agg_daily_sales",
        "agg_product_performance",
        "agg_customer_metrics"
    ]
    for t in required:
        assert t in tables

def test_surrogate_keys():
    cols = inspector.get_columns("fact_sales", schema="warehouse")
    names = [c["name"] for c in cols]
    assert "customer_key" in names
    assert "product_key" in names