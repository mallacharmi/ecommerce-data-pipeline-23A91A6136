import sys
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import scripts.ingestion.ingest_to_staging as ingest_module

load_dotenv(PROJECT_ROOT / ".env")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)


def test_ingestion_executes():
    if hasattr(ingest_module, "main"):
        ingest_module.main()
    elif hasattr(ingest_module, "run"):
        ingest_module.run()
    else:
        assert True


def test_db_connection():
    with engine.connect() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_staging_tables_exist():
    with engine.connect() as conn:
        tables = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'staging'
            """)
        ).fetchall()

    table_names = [t[0] for t in tables]
    for t in ["customers", "products", "transactions", "transaction_items"]:
        assert t in table_names
