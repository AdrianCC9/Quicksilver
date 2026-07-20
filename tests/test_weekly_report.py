from __future__ import annotations

from datetime import date

from analytics.weekly_report import generate_weekly_performance_report
from storage.local_mysql_storage import LocalMySQLStorage


def test_generate_weekly_report_writes_files_and_logs_run(tmp_path):
    storage = LocalMySQLStorage(f"sqlite:///{tmp_path / 'report.db'}")
    storage.create_tables()

    report = generate_weekly_performance_report(
        storage=storage,
        as_of_date=date(2026, 1, 7),
        output_root=tmp_path / "reports",
    )

    assert report.markdown_path.exists()
    assert report.pdf_path.exists()
    assert report.csv_paths["performance_summary"].exists()
    assert "Quicksilver Weekly Performance Report" in report.markdown_path.read_text()
    assert len(storage.fetch_dashboard_table("report_runs")) == 1

    storage.close()
