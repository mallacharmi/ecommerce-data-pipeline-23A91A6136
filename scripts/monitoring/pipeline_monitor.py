import json
import time
import statistics
from pathlib import Path
from datetime import datetime, timezone
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------
# Environment & Paths
# -------------------------------------------------
load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)

PIPELINE_REPORT_DIR = Path("data/processed")
MONITORING_OUTPUT = PIPELINE_REPORT_DIR / "monitoring_report.json"

# -------------------------------------------------
# Utility Functions
# -------------------------------------------------
def now_utc():
    return datetime.now(timezone.utc)

def hours_diff(t1, t2):
    return abs((t1 - t2).total_seconds()) / 3600

def load_latest_pipeline_report():
    reports = sorted(
        PIPELINE_REPORT_DIR.glob("pipeline_execution_report.json"),
        reverse=True
    )
    if not reports:
        return None
    return json.load(open(reports[0]))

# -------------------------------------------------
# Monitoring Checks
# -------------------------------------------------
def check_last_execution():
    report = load_latest_pipeline_report()
    if not report:
        return {
            "status": "critical",
            "last_run": None,
            "hours_since_last_run": None,
            "threshold_hours": 25
        }

    last_run_time = datetime.fromisoformat(report["end_time"])

    # 🔧 FIX: make timezone-aware
    if last_run_time.tzinfo is None:
        last_run_time = last_run_time.replace(tzinfo=timezone.utc)

    hours_since = hours_diff(now_utc(), last_run_time)

    status = "ok"
    if hours_since > 25:
        status = "critical"
    elif hours_since > 24:
        status = "warning"

    return {
        "status": status,
        "last_run": last_run_time.isoformat(),
        "hours_since_last_run": round(hours_since, 2),
        "threshold_hours": 25
    }

def check_data_freshness(conn):
    query = """
    SELECT
        (SELECT MAX(loaded_at) FROM staging.customers) AS staging_latest,
        (SELECT MAX(created_at) FROM production.customers) AS production_latest,
        (SELECT MAX(created_at) FROM warehouse.fact_sales) AS warehouse_latest
    """
    result = conn.execute(text(query)).mappings().first()

    now = now_utc()

    lags = []
    for k in result.values():
        if k:
            if k.tzinfo is None:
                k = k.replace(tzinfo=timezone.utc)
            lags.append(hours_diff(now, k))

    max_lag = max(lags) if lags else None

    status = "ok"
    if max_lag and max_lag > 2:
        status = "critical"
    elif max_lag and max_lag > 1:
        status = "warning"

    return {
        "status": status,
        "staging_latest_record": str(result["staging_latest"]),
        "production_latest_record": str(result["production_latest"]),
        "warehouse_latest_record": str(result["warehouse_latest"]),
        "max_lag_hours": round(max_lag, 2) if max_lag else None
    }

def check_volume_anomalies(conn):
    query = """
    SELECT transaction_date, COUNT(*) AS txn_count
    FROM production.transactions
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY transaction_date
    ORDER BY transaction_date
    """
    rows = conn.execute(text(query)).fetchall()
    if len(rows) < 5:
        return {
            "status": "ok",
            "expected_range": None,
            "actual_count": None,
            "anomaly_detected": False,
            "anomaly_type": None
        }

    counts = [r[1] for r in rows]
    mean = statistics.mean(counts)
    std = statistics.stdev(counts)
    today_count = counts[-1]

    lower = mean - (3 * std)
    upper = mean + (3 * std)

    anomaly = today_count < lower or today_count > upper

    anomaly_type = None
    if anomaly:
        anomaly_type = "spike" if today_count > upper else "drop"

    return {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(lower)}-{int(upper)}",
        "actual_count": today_count,
        "anomaly_detected": anomaly,
        "anomaly_type": anomaly_type
    }

def check_data_quality(conn):
    orphan_query = """
    SELECT COUNT(*) FROM production.transactions t
    LEFT JOIN production.customers c
    ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
    """
    orphans = conn.execute(text(orphan_query)).scalar()

    null_query = """
    SELECT COUNT(*) FROM production.customers
    WHERE email IS NULL OR customer_id IS NULL
    """
    nulls = conn.execute(text(null_query)).scalar()

    score = max(0, 100 - (orphans * 5 + nulls * 2))

    status = "ok" if score >= 95 else "degraded"

    return {
        "status": status,
        "quality_score": score,
        "orphan_records": orphans,
        "null_violations": nulls
    }

def check_database_health(conn):
    start = time.time()
    conn.execute(text("SELECT 1"))
    response_time = (time.time() - start) * 1000

    connections = conn.execute(
        text("SELECT COUNT(*) FROM pg_stat_activity")
    ).scalar()

    return {
        "status": "ok",
        "response_time_ms": round(response_time, 2),
        "connections_active": connections
    }

# -------------------------------------------------
# Main Monitoring Runner
# -------------------------------------------------
def run_monitoring():
    alerts = []

    with engine.connect() as conn:
        last_exec = check_last_execution()
        freshness = check_data_freshness(conn)
        volume = check_volume_anomalies(conn)
        quality = check_data_quality(conn)
        db_health = check_database_health(conn)

    checks = {
        "last_execution": last_exec,
        "data_freshness": freshness,
        "data_volume_anomalies": volume,
        "data_quality": quality,
        "database_connectivity": db_health
    }

    for name, check in checks.items():
        if check["status"] in ["warning", "critical", "anomaly_detected"]:
            alerts.append({
                "severity": "critical" if check["status"] == "critical" else "warning",
                "check": name,
                "message": f"Issue detected in {name}",
                "timestamp": now_utc().isoformat()
            })

    overall_score = 100
    if alerts:
        overall_score -= len(alerts) * 10

    report = {
        "monitoring_timestamp": now_utc().isoformat(),
        "pipeline_health": "healthy" if overall_score >= 90 else "degraded",
        "checks": checks,
        "alerts": alerts,
        "overall_health_score": max(0, overall_score)
    }

    PIPELINE_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MONITORING_OUTPUT, "w") as f:
        json.dump(report, f, indent=4)

    print("🩺 Monitoring completed successfully")

# -------------------------------------------------
if __name__ == "__main__":
    run_monitoring()