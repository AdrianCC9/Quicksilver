from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from analytics.weekly_report import generate_weekly_performance_report  # noqa: E402
from config import settings  # noqa: E402
from storage.local_mysql_storage import LocalMySQLStorage  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Quicksilver weekly CSV/Markdown/PDF performance reports."
    )
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--as-of-date", help="YYYY-MM-DD, defaults to today.")
    parser.add_argument("--output-dir", default=settings.report_output_dir)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    as_of_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    storage = LocalMySQLStorage(args.database_url)
    try:
        storage.create_tables()
        report = generate_weekly_performance_report(
            storage=storage,
            as_of_date=as_of_date,
            days=args.days,
            output_root=args.output_dir,
        )
    finally:
        storage.close()

    print("Quicksilver weekly report generated")
    print(f"- period: {report.period_start} to {report.period_end}")
    print(f"- output_dir: {report.output_dir}")
    print(f"- markdown: {report.markdown_path}")
    print(f"- pdf: {report.pdf_path}")
    for name, path in sorted(report.csv_paths.items()):
        print(f"- csv[{name}]: {path}")


if __name__ == "__main__":
    main()
