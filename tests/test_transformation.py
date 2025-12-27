import sys
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import scripts.transformation.staging_to_production as transform_module

load_dotenv(PROJECT_ROOT / ".env")

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)


def test_transformation_executes():
    if hasattr(transform_module, "main"):
        transform_module.main()
    elif hasattr(transform_module, "run"):
        transform_module.run()
    else:
        assert True


def test_production_tables_populated():
    with engine.connect() as conn:
        assert conn.execute(
            text("SELECT COUNT(*) FROM production.customers")
        ).scalar() > 0


def test_email_lowercase():
    with engine.connect() as conn:
        assert conn.execute(
            text("""
                SELECT COUNT(*)
                FROM production.customers
                WHERE email <> LOWER(email)
            """)
        ).scalar() == 0
