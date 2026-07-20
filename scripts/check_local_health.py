from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from alerts.local_health import (  # noqa: E402
    LocalPipelineHealthMonitor,
    format_local_health_alerts,
    send_local_health_alerts,
)
from config import settings  # noqa: E402
from storage.local_mysql_storage import LocalMySQLStorage  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check Quicksilver local pipeline health and persist alerts."
    )
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--send", action="store_true", help="Send Slack/email alerts if enabled.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    storage = LocalMySQLStorage(args.database_url)
    try:
        storage.create_tables()
        run_logs = storage.fetch_dashboard_table("pipeline_run_logs")
        latest_success = _latest_successful_finish(run_logs)
        alerts = LocalPipelineHealthMonitor().evaluate_staleness(latest_success)
        storage.resolve_health_alerts(LocalPipelineHealthMonitor.MONITORED_ALERT_TYPES)
        storage.save_health_alerts([alert.to_row() for alert in alerts])
        if args.send:
            send_local_health_alerts(alerts)
        print(format_local_health_alerts(alerts))
    finally:
        storage.close()


def _latest_successful_finish(run_logs: pd.DataFrame) -> datetime | None:
    if run_logs.empty or "finished_at_utc" not in run_logs.columns:
        return None

    successful = run_logs[run_logs["status"] == "success"].copy()
    if successful.empty:
        return None

    successful["finished_at_utc"] = pd.to_datetime(
        successful["finished_at_utc"],
        utc=True,
        errors="coerce",
    )
    successful = successful.dropna(subset=["finished_at_utc"])
    if successful.empty:
        return None

    value = successful["finished_at_utc"].max()
    if pd.isna(value):
        return None
    return value.to_pydatetime().astimezone(timezone.utc)


if __name__ == "__main__":
    main()
