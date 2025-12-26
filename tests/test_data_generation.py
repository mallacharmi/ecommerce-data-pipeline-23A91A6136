import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("data/raw")


def test_csv_files_exist():
    assert (DATA_DIR / "customers.csv").exists()
    assert (DATA_DIR / "products.csv").exists()
    assert (DATA_DIR / "transactions.csv").exists()
    assert (DATA_DIR / "transaction_items.csv").exists()


def test_row_counts():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    products = pd.read_csv(DATA_DIR / "products.csv")
    transactions = pd.read_csv(DATA_DIR / "transactions.csv")
    items = pd.read_csv(DATA_DIR / "transaction_items.csv")

    assert len(customers) > 0
    assert len(products) > 0
    assert len(transactions) > 0
    assert len(items) > 0


def test_required_columns():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    assert "customer_id" in customers.columns
    assert "email" in customers.columns

    products = pd.read_csv(DATA_DIR / "products.csv")
    assert "product_id" in products.columns
    assert "price" in products.columns

    items = pd.read_csv(DATA_DIR / "transaction_items.csv")
    assert "line_total" in items.columns


def test_id_format():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    assert customers["customer_id"].str.startswith("CUST").all()

    products = pd.read_csv(DATA_DIR / "products.csv")
    assert products["product_id"].str.startswith("PROD").all()


def test_referential_integrity():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    transactions = pd.read_csv(DATA_DIR / "transactions.csv")

    assert transactions["customer_id"].isin(customers["customer_id"]).all()


# ✅ FIXED FLOATING-POINT SAFE TEST
def test_line_total_calculation():
    items = pd.read_csv(DATA_DIR / "transaction_items.csv")

    recalculated = (
        items["quantity"]
        * items["unit_price"]
        * (1 - items["discount_percentage"] / 100)
    )

    assert np.allclose(items["line_total"], recalculated, atol=0.01)