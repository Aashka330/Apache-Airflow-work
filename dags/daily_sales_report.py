"""Daily local sales reporting workflow."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = PROJECT_ROOT / "output" / "daily_sales_report.json"


@dag(
    dag_id="daily_sales_report",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md="""
    # Daily sales report

    Extract local sales records, aggregate revenue by region, write the report,
    and validate the generated artifact.
    """,
    tags=["example", "local", "reporting"],
)
def daily_sales_report():
    @task
    def extract_sales() -> list[dict[str, object]]:
        """Return the records that would normally come from an upstream source."""
        return [
            {"order_id": "A-1001", "region": "north", "amount": 125.50},
            {"order_id": "A-1002", "region": "south", "amount": 89.00},
            {"order_id": "A-1003", "region": "north", "amount": 210.00},
            {"order_id": "A-1004", "region": "west", "amount": 76.25},
        ]

    @task
    def transform_sales(records: list[dict[str, object]]) -> dict[str, object]:
        """Aggregate order count and revenue by region."""
        revenue_by_region: dict[str, float] = {}
        for record in records:
            region = str(record["region"])
            revenue_by_region[region] = revenue_by_region.get(region, 0.0) + float(
                record["amount"]
            )

        return {
            "generated_at": datetime.now().astimezone().isoformat(),
            "order_count": len(records),
            "total_revenue": round(sum(revenue_by_region.values()), 2),
            "revenue_by_region": {
                region: round(amount, 2)
                for region, amount in sorted(revenue_by_region.items())
            },
        }

    @task
    def load_report(report: dict[str, object]) -> str:
        """Write the transformed report to the local output directory."""
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return str(REPORT_PATH)

    @task
    def validate_report(report_path: str) -> None:
        """Fail the run if the report is missing or internally inconsistent."""
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
        revenue_by_region = report["revenue_by_region"]
        if report["order_count"] != 4:
            raise ValueError("Expected four orders in the daily report")
        if round(sum(revenue_by_region.values()), 2) != report["total_revenue"]:
            raise ValueError("Regional revenue does not match total revenue")

    records = extract_sales()
    report = transform_sales(records)
    report_path = load_report(report)
    validate_report(report_path)


daily_sales_report()