import json
from pathlib import Path

REPORT = Path("data/processed/data_quality_report.json")



def test_quality_report_exists():
    assert REPORT.exists()


def test_quality_score_range():
    report = json.load(open(REPORT))
    score = report["data_quality_summary"]["quality_score"]
    assert 0 <= score <= 100