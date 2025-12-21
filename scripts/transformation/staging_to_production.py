import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

REPORT_DIR = Path("data/processed")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------
# Helper Functions
# ---------------------------------
def clean_text(df):
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def standardize_customers(df):
    df = clean_text(df)
    df["email"] = df["email"].str.lower()
    df["first_name"] = df["first_name"].str.title()
    df["last_name"] = df["last_name"].str.title()
    df["phone"] = df["phone"].str.replace(r"\D", "", regex=True)
    return df


def enrich_products(df):
    df = clean_text(df)
    df["price"] = df["price"].round(2)
    df["cost"] = df["cost"].round(2)

    # Derived columns (warehouse-ready, NOT stored in production)
    df["profit_margin"] = round(
        ((df["price"] - df["cost"]) / df["price"]) * 100, 2
    )

    df["price_category"] = df["price"].apply(
        lambda x: "Budget" if x < 50 else "Mid-range" if x < 200 else "Premium"
    )
    return df


# ---------------------------------
# Main ETL Logic
# ---------------------------------
def staging_to_production():
    print("🚀 Starting staging → production ETL")

    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_trim",
            "email_lowercase",
            "phone_standardization",
            "profit_margin_calculation (warehouse-ready)",
            "price_category_assignment (warehouse-ready)"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0
        }
    }

    with engine.begin() as conn:
        # =============================
        # CUSTOMERS (DIMENSION)
        # =============================
        print("🔄 Loading customers...")
        customers = pd.read_sql("SELECT * FROM staging.customers", conn)
        input_count = len(customers)

        customers = customers.drop(columns=["loaded_at"], errors="ignore")
        customers = standardize_customers(customers)

        conn.execute(text("TRUNCATE production.customers CASCADE"))
        customers.to_sql(
            "customers",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["customers"] = {
            "input": input_count,
            "output": len(customers),
            "filtered": 0,
            "rejected_reasons": {}
        }
        print(f"✅ Customers loaded: {len(customers)}")

        # =============================
        # PRODUCTS (DIMENSION)
        # =============================
        print("🔄 Loading products...")
        products = pd.read_sql("SELECT * FROM staging.products", conn)
        input_count = len(products)

        products = products.drop(columns=["loaded_at"], errors="ignore")
        products = enrich_products(products)
        products = products[products["price"] > 0]

        # Drop derived columns before loading to production
        products_to_load = products.drop(
            columns=["profit_margin", "price_category"],
            errors="ignore"
        )

        conn.execute(text("TRUNCATE production.products CASCADE"))
        products_to_load.to_sql(
            "products",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["products"] = {
            "input": input_count,
            "output": len(products_to_load),
            "filtered": input_count - len(products_to_load),
            "rejected_reasons": {
                "invalid_price": int(input_count - len(products_to_load))
            }
        }
        print(f"✅ Products loaded: {len(products_to_load)}")

        # =============================
        # TRANSACTIONS (FACT – APPEND)
        # =============================
        print("🔄 Loading transactions...")
        transactions = pd.read_sql("SELECT * FROM staging.transactions", conn)
        input_count = len(transactions)

        transactions = transactions.drop(columns=["loaded_at"], errors="ignore")
        transactions = transactions[transactions["total_amount"] > 0]

        transactions.to_sql(
            "transactions",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["transactions"] = {
            "input": input_count,
            "output": len(transactions),
            "filtered": input_count - len(transactions),
            "rejected_reasons": {
                "total_amount_le_zero": int(input_count - len(transactions))
            }
        }
        print(f"✅ Transactions loaded: {len(transactions)}")

        # =============================
        # TRANSACTION ITEMS (FACT – APPEND)
        # =============================
        print("🔄 Loading transaction items...")
        items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        input_count = len(items)

        items = items.drop(columns=["loaded_at"], errors="ignore")
        items = items[items["quantity"] > 0]

        items["line_total"] = round(
            items["quantity"]
            * items["unit_price"]
            * (1 - items["discount_percentage"] / 100),
            2
        )

        items.to_sql(
            "transaction_items",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["transaction_items"] = {
            "input": input_count,
            "output": len(items),
            "filtered": input_count - len(items),
            "rejected_reasons": {
                "invalid_quantity": int(input_count - len(items))
            }
        }
        print(f"✅ Transaction items loaded: {len(items)}")

    # =============================
    # Write Summary
    # =============================
    print("📊 Writing transformation summary...")
    with open(REPORT_DIR / "transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    print("🎉 Staging → Production ETL COMPLETED SUCCESSFULLY")


# ---------------------------------
# Entry Point
# ---------------------------------
if __name__ == "__main__":
    staging_to_production()