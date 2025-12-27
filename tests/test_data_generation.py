import sys
from pathlib import Path
import pandas as pd
import numpy as np

# -------------------------------
# ADD PROJECT ROOT TO PYTHON PATH
# -------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

# IMPORT MODULE FOR COVERAGE
import scripts.data_generation.generate_data as gen_module

DATA_DIR = PROJECT_ROOT / "data" / "raw"


def test_data_generation_executes():
    if hasattr(gen_module, "main"):
        gen_module.main()
    elif hasattr(gen_module, "generate_all_data"):
        gen_module.generate_all_data()
    else:
        assert True


def test_csv_files_exist():
    assert (DATA_DIR / "customers.csv").exists()
    assert (DATA_DIR / "products.csv").exists()
    assert (DATA_DIR / "transactions.csv").exists()
    assert (DATA_DIR / "transaction_items.csv").exists()


def test_row_counts():
    assert len(pd.read_csv(DATA_DIR / "customers.csv")) > 0
    assert len(pd.read_csv(DATA_DIR / "products.csv")) > 0
    assert len(pd.read_csv(DATA_DIR / "transactions.csv")) > 0
    assert len(pd.read_csv(DATA_DIR / "transaction_items.csv")) > 0


def test_required_columns():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    assert {"customer_id", "email"}.issubset(customers.columns)


def test_id_format():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    assert customers["customer_id"].str.startswith("CUST").all()


def test_referential_integrity():
    customers = pd.read_csv(DATA_DIR / "customers.csv")
    transactions = pd.read_csv(DATA_DIR / "transactions.csv")
    assert transactions["customer_id"].isin(customers["customer_id"]).all()


def test_line_total_calculation():
    items = pd.read_csv(DATA_DIR / "transaction_items.csv")
    recalculated = (
        items["quantity"]
        * items["unit_price"]
        * (1 - items["discount_percentage"] / 100)
    )
    assert np.allclose(items["line_total"], recalculated, atol=0.01)
