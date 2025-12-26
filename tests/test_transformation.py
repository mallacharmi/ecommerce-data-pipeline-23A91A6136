from sqlalchemy import create_engine, text
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

def test_production_tables_populated():
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM production.customers")
        ).scalar()
        assert count > 0

def test_email_lowercase():
    with engine.connect() as conn:
        invalid = conn.execute(
            text("SELECT COUNT(*) FROM production.customers WHERE email <> LOWER(email)")
        ).scalar()
        assert invalid == 0

def test_no_orphan_transactions():
    with engine.connect() as conn:
        orphans = conn.execute(
            text("""
                SELECT COUNT(*) FROM production.transactions t
                LEFT JOIN production.customers c
                ON t.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """)
        ).scalar()
        assert orphans == 0

def test_idempotency():
    with engine.connect() as conn:
        before = conn.execute(
            text("SELECT COUNT(*) FROM production.customers")
        ).scalar()

        after = conn.execute(
            text("SELECT COUNT(*) FROM production.customers")
        ).scalar()

        assert before == after