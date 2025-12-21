import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime
import yaml
from pathlib import Path

fake = Faker()

# -----------------------------
# Load configuration
# -----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Customers
# -----------------------------
def generate_customers(num_customers: int):
    customers = []
    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]

    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"customer{i}@example.com",
            "phone": fake.msisdn(),
            "registration_date": fake.date_between(start_date="-2y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(age_groups)
        })

    return pd.DataFrame(customers)


# -----------------------------
# Products
# -----------------------------
def generate_products(num_products: int):
    categories = {
        "Electronics": ["Mobile", "Laptop", "Headphones"],
        "Clothing": ["Shirt", "Jeans", "Dress"],
        "Home & Kitchen": ["Furniture", "Cookware"],
        "Books": ["Fiction", "Education"],
        "Sports": ["Fitness", "Outdoor"],
        "Beauty": ["Skincare", "Makeup"]
    }

    products = []

    for i in range(1, num_products + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])

        price = round(random.uniform(200, 5000), 2)
        cost = round(price * random.uniform(0.6, 0.85), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": f"{fake.word().title()} {sub_category}",
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)


# -----------------------------
# Transactions
# -----------------------------
def generate_transactions(num_transactions: int, customers_df):
    transactions = []
    customer_ids = customers_df["customer_id"].tolist()

    payment_methods = [
        "Credit Card", "Debit Card", "UPI",
        "Cash on Delivery", "Net Banking"
    ]

    start_date = datetime.strptime(
        config["data_generation"]["transaction_date_range"]["start_date"], "%Y-%m-%d"
    )
    end_date = datetime.strptime(
        config["data_generation"]["transaction_date_range"]["end_date"], "%Y-%m-%d"
    )

    for i in range(1, num_transactions + 1):
        tx_time = fake.date_time_between(start_date=start_date, end_date=end_date)

        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": tx_time.date(),
            "transaction_time": tx_time.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0
        })

    return pd.DataFrame(transactions)


# -----------------------------
# Transaction Items
# -----------------------------
def generate_transaction_items(transactions_df, products_df):
    items = []
    item_counter = 1

    product_lookup = products_df.set_index("product_id").to_dict("index")

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        chosen_products = random.sample(list(product_lookup.keys()), num_items)

        txn_total = 0.0

        for pid in chosen_products:
            quantity = random.randint(1, 3)
            unit_price = product_lookup[pid]["price"]
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * unit_price * (1 - discount / 100), 2
            )

            txn_total += line_total

            items.append({
                "item_id": f"ITEM{item_counter:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": pid,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total
            })

            item_counter += 1

        transactions_df.loc[
            transactions_df["transaction_id"] == txn["transaction_id"],
            "total_amount"
        ] = round(txn_total, 2)

    return pd.DataFrame(items)


# -----------------------------
# Referential Integrity Validation
# -----------------------------
def validate_referential_integrity(customers, products, transactions, items):
    orphan_customer_txn = ~transactions["customer_id"].isin(customers["customer_id"])
    orphan_item_txn = ~items["transaction_id"].isin(transactions["transaction_id"])
    orphan_item_product = ~items["product_id"].isin(products["product_id"])

    total_orphans = (
        orphan_customer_txn.sum()
        + orphan_item_txn.sum()
        + orphan_item_product.sum()
    )

    return {
        "orphan_records": int(total_orphans),
        "constraint_violations": 0,
        "data_quality_score": 100 if total_orphans == 0 else max(0, 100 - total_orphans)
    }


# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    customers_df = generate_customers(config["data_generation"]["customers"])
    products_df = generate_products(config["data_generation"]["products"])
    transactions_df = generate_transactions(
        config["data_generation"]["transactions"], customers_df
    )
    items_df = generate_transaction_items(transactions_df, products_df)

    customers_df.to_csv(DATA_DIR / "customers.csv", index=False)
    products_df.to_csv(DATA_DIR / "products.csv", index=False)
    transactions_df.to_csv(DATA_DIR / "transactions.csv", index=False)
    items_df.to_csv(DATA_DIR / "transaction_items.csv", index=False)

    integrity_report = validate_referential_integrity(
        customers_df, products_df, transactions_df, items_df
    )

    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions_df),
            "transaction_items": len(items_df)
        },
        "transaction_date_range": config["data_generation"]["transaction_date_range"],
        "data_quality": integrity_report
    }

    with open(DATA_DIR / "generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("✅ Data generation completed successfully")