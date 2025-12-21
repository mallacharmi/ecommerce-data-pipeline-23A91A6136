import json
from datetime import datetime
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# -----------------------------
# DB Connection
# -----------------------------
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


# -----------------------------
# Helper Function
# -----------------------------
def run_scalar(query):
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar()


# -----------------------------
# Quality Checks
# -----------------------------
def run_quality_checks():
    results = {}

    # Completeness
    nulls = run_scalar("""
        SELECT COUNT(*) FROM production.customers
        WHERE customer_id IS NULL OR email IS NULL
    """)
    missing_items = run_scalar("""
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.transaction_items ti
        ON t.transaction_id = ti.transaction_id
        WHERE ti.transaction_id IS NULL
    """)

    results["null_checks"] = {
        "status": "passed" if nulls == 0 else "failed",
        "tables_checked": ["customers", "transactions"],
        "null_violations": int(nulls + missing_items),
        "details": {
            "customers.email_or_id_null": int(nulls),
            "transactions_without_items": int(missing_items)
        }
    }

    # Uniqueness
    dup_emails = run_scalar("""
        SELECT COUNT(*) FROM (
            SELECT email FROM production.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) x
    """)

    results["duplicate_checks"] = {
        "status": "passed" if dup_emails == 0 else "failed",
        "duplicates_found": int(dup_emails),
        "details": {"customers.email": int(dup_emails)}
    }

    # Referential Integrity
    orphan_txn = run_scalar("""
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)

    orphan_item_txn = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    orphan_item_prod = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.products p
        ON ti.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)

    total_orphans = orphan_txn + orphan_item_txn + orphan_item_prod

    results["referential_integrity"] = {
        "status": "passed" if total_orphans == 0 else "failed",
        "orphan_records": int(total_orphans),
        "details": {
            "transactions.customer": int(orphan_txn),
            "items.transaction": int(orphan_item_txn),
            "items.product": int(orphan_item_prod)
        }
    }

    # Range & Business Rules
    invalid_ranges = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items
        WHERE quantity <= 0 OR discount_percentage < 0 OR discount_percentage > 100
    """)

    invalid_price = run_scalar("""
        SELECT COUNT(*) FROM production.products
        WHERE price <= 0 OR cost <= 0 OR cost >= price
    """)

    results["range_checks"] = {
        "status": "passed" if invalid_ranges + invalid_price == 0 else "failed",
        "violations": int(invalid_ranges + invalid_price),
        "details": {
            "transaction_items.range": int(invalid_ranges),
            "products.cost_price": int(invalid_price)
        }
    }

    # Consistency
    line_mismatch = run_scalar("""
        SELECT COUNT(*) FROM production.transaction_items
        WHERE ROUND(quantity * unit_price * (1 - discount_percentage/100), 2) != line_total
    """)

    txn_mismatch = run_scalar("""
        SELECT COUNT(*) FROM (
            SELECT t.transaction_id
            FROM production.transactions t
            JOIN production.transaction_items ti
            ON t.transaction_id = ti.transaction_id
            GROUP BY t.transaction_id, t.total_amount
            HAVING ROUND(SUM(ti.line_total), 2) != t.total_amount
        ) x
    """)

    results["data_consistency"] = {
        "status": "passed" if line_mismatch + txn_mismatch == 0 else "failed",
        "mismatches": int(line_mismatch + txn_mismatch),
        "details": {
            "line_total": int(line_mismatch),
            "transaction_total": int(txn_mismatch)
        }
    }

    # Accuracy
    future_txns = run_scalar("""
        SELECT COUNT(*) FROM production.transactions
        WHERE transaction_date > CURRENT_DATE
    """)

    reg_after_txn = run_scalar("""
        SELECT COUNT(*) FROM production.transactions t
        JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.registration_date > t.transaction_date
    """)

    accuracy_violations = future_txns + reg_after_txn

    # -----------------------------
    # Scoring (Weighted)
    # -----------------------------
    score = 100
    score -= total_orphans * 5
    score -= (invalid_ranges + invalid_price) * 2
    score -= (line_mismatch + txn_mismatch) * 2
    score -= accuracy_violations * 1

    score = max(0, score)

    grade = (
        "A" if score >= 90 else
        "B" if score >= 80 else
        "C" if score >= 70 else
        "D" if score >= 60 else "F"
    )

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": results,
        "overall_quality_score": score,
        "quality_grade": grade
    }

    return report


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    report = run_quality_checks()
    output_file = REPORT_DIR / "quality_report.json"

    with open(output_file, "w") as f:
        json.dump(report, f, indent=4)

    print("✅ Data quality checks completed")