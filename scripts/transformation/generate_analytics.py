import pandas as pd
import json
import time
from pathlib import Path
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from datetime import datetime

# ---------------------------------------------------
# Load environment variables
# ---------------------------------------------------
load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)

OUTPUT_DIR = Path("data/processed/analytics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------
# Helper functions
# ---------------------------------------------------
def execute_query(conn, sql):
    start = time.time()
    df = pd.read_sql_query(text(sql), conn)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    return df, elapsed_ms


def export_to_csv(df, filename):
    df.to_csv(OUTPUT_DIR / filename, index=False)


# ---------------------------------------------------
# Main Analytics Generator
# ---------------------------------------------------
def generate_analytics():
    print("📊 Generating analytics...")

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    sql_file = Path("sql/queries/analytical_queries.sql")
    raw_sql = sql_file.read_text()

    # Split queries safely
    queries = [q.strip() for q in raw_sql.split(";") if q.strip().upper().startswith("SELECT")]

    total_start = time.time()

    with engine.connect() as conn:
        for idx, query in enumerate(queries, start=1):
            query_name = f"query{idx}"
            print(f"▶ Executing {query_name}...")

            df, exec_time = execute_query(conn, query)

            export_to_csv(df, f"{query_name}.csv")

            summary["query_results"][query_name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": exec_time
            }

        summary["queries_executed"] = len(queries)
        summary["total_execution_time_seconds"] = round(time.time() - total_start, 2)

    # Write summary JSON
    with open(OUTPUT_DIR / "analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    print("✅ Analytics generation completed successfully")


if __name__ == "__main__":
    generate_analytics()