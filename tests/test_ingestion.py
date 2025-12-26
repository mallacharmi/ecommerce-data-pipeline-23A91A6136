import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------
# FORCE LOAD .env FROM PROJECT ROOT (FIX)
# -------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_PATH)

# -------------------------------------------------
# SAFETY CHECK (IMPORTANT)
# -------------------------------------------------
assert os.getenv("DB_PORT") is not None, "DB_PORT is not loaded from .env"

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)


def test_db_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1


def test_staging_tables_exist():
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'staging'
            """)
        ).fetchall()

        tables = [r[0] for r in result]

        assert "customers" in tables
        assert "products" in tables
        assert "transactions" in tables
        assert "transaction_items" in tables


def test_loaded_at_column():
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT loaded_at
                FROM staging.customers
                LIMIT 1
            """)
        ).fetchone()

        assert result[0] is not None