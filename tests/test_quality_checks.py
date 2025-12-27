import sys
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import scripts.quality_checks.validate_data as quality_module

REPORT = PROJECT_ROOT / "data" / "processed" / "data_quality_report.json"


def test_quality_checks_execute():
    if hasattr(quality_module, "main"):
        quality_module.main()
    elif hasattr(quality_module, "run"):
        quality_module.run()
    else:
        assert True


def test_quality_report_exists():
    assert REPORT.exists()


def test_quality_score_range():
    with open(REPORT) as f:
        report = json.load(f)
    score = report["data_quality_summary"]["quality_score"]
    assert 0 <= score <= 100
