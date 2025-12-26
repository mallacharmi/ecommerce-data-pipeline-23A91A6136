import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
import traceback

# ----------------------------------------------------
# FIX PYTHON IMPORT PATH (CRITICAL FOR WINDOWS)
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# ----------------------------------------------------
# IMPORT PIPELINE STEPS (SAFE IMPORTS)
# ----------------------------------------------------
from scripts.data_generation.generate_data import generate_all_data
from scripts.ingestion.ingest_to_staging import ingest_to_staging
from scripts.quality_checks.validate_data import run_quality_checks
from scripts.transformation.staging_to_production import staging_to_production
from scripts.transformation.load_warehouse import load_warehouse
from scripts.transformation.generate_analytics import generate_analytics

# ----------------------------------------------------
# DIRECTORIES
# ----------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_DIR = PROJECT_ROOT / "data" / "processed"
LOG_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------
# LOGGING CONFIGURATION (NO EMOJIS)
# ----------------------------------------------------
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"pipeline_orchestrator_{timestamp}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

error_logger = logging.getLogger("pipeline_errors")
error_handler = logging.FileHandler(LOG_DIR / "pipeline_errors.log", encoding="utf-8")
error_logger.addHandler(error_handler)

# ----------------------------------------------------
# RETRY CONFIG
# ----------------------------------------------------
MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# ----------------------------------------------------
# PIPELINE STEP WRAPPER
# ----------------------------------------------------
def run_step(step_name, step_function, report):
    retries = 0
    start = time.time()

    while retries < MAX_RETRIES:
        try:
            logging.info(f"Starting step: {step_name}")
            step_function()

            duration = round(time.time() - start, 2)
            report["steps_executed"][step_name] = {
                "status": "success",
                "duration_seconds": duration,
                "records_processed": None,
                "error_message": None,
                "retry_attempts": retries
            }

            logging.info(f"Completed step: {step_name} in {duration}s")
            return True

        except Exception as e:
            retries += 1
            error_msg = str(e)

            error_logger.error(f"{step_name} failed: {error_msg}")
            error_logger.error(traceback.format_exc())

            if retries >= MAX_RETRIES:
                report["steps_executed"][step_name] = {
                    "status": "failed",
                    "duration_seconds": round(time.time() - start, 2),
                    "records_processed": None,
                    "error_message": error_msg,
                    "retry_attempts": retries
                }
                return False

            logging.warning(
                f"{step_name} retry {retries}/{MAX_RETRIES} "
                f"(waiting {BACKOFF_SECONDS[retries - 1]}s)"
            )
            time.sleep(BACKOFF_SECONDS[retries - 1])

# ----------------------------------------------------
# MAIN PIPELINE FUNCTION
# ----------------------------------------------------
def run_pipeline():
    pipeline_id = f"PIPE_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    start_time = datetime.now(timezone.utc)

    report = {
        "pipeline_execution_id": pipeline_id,
        "start_time": start_time.isoformat(),
        "end_time": None,
        "total_duration_seconds": None,
        "status": "running",
        "steps_executed": {},
        "data_quality_summary": {},
        "errors": [],
        "warnings": []
    }

    steps = [
        ("data_generation", generate_all_data),
        ("data_ingestion", ingest_to_staging),
        ("data_quality_checks", run_quality_checks),
        ("staging_to_production", staging_to_production),
        ("warehouse_load", load_warehouse),
        ("analytics_generation", generate_analytics),
    ]

    for step_name, step_fn in steps:
        success = run_step(step_name, step_fn, report)
        if not success:
            report["status"] = "failed"
            report["errors"].append(f"{step_name} failed")
            break

    end_time = datetime.now(timezone.utc)
    report["end_time"] = end_time.isoformat()
    report["total_duration_seconds"] = round(
        (end_time - start_time).total_seconds(), 2
    )

    if report["status"] != "failed":
        report["status"] = "success"

    # ------------------------------------------------
    # WRITE PIPELINE REPORT
    # ------------------------------------------------
    report_path = REPORT_DIR / "pipeline_execution_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    logging.info("Pipeline execution report generated")
    logging.info(f"Pipeline finished with status: {report['status']}")

# ----------------------------------------------------
# ENTRY POINT (CRITICAL FOR PYTEST)
# ----------------------------------------------------
if __name__ == "__main__":
    logging.info("Starting End-to-End ETL Pipeline")
    run_pipeline()
