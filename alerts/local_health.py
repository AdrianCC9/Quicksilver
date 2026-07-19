from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import logging
import smtplib
from email.message import EmailMessage
from typing import Any

import requests

from config import settings


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LocalHealthAlert:
    alert_key: str
    severity: str
    alert_type: str
    message: str
    run_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    detected_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "open"

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


class LocalPipelineHealthMonitor:
    MONITORED_ALERT_TYPES = {
        "low_headline_coverage",
        "low_insight_coverage",
        "high_synthetic_quote_usage",
        "portfolio_equity_invalid",
        "pipeline_failed",
        "pipeline_never_finished",
        "pipeline_stale",
    }

    def evaluate_success(
        self,
        summary: dict[str, Any],
    ) -> list[LocalHealthAlert]:
        run_id = str(summary.get("run_id") or "unknown")
        alerts: list[LocalHealthAlert] = []

        raw_count = int(summary.get("raw_headlines_collected") or 0)
        if raw_count < settings.health_min_raw_headlines_per_run:
            alerts.append(
                self._alert(
                    run_id=run_id,
                    alert_type="low_headline_coverage",
                    severity="warning",
                    message=(
                        f"Only {raw_count} raw headlines were collected; "
                        f"expected at least {settings.health_min_raw_headlines_per_run}."
                    ),
                    details={
                        "raw_headlines_collected": raw_count,
                        "threshold": settings.health_min_raw_headlines_per_run,
                    },
                )
            )

        insight_count = int(summary.get("insights_generated") or 0)
        if insight_count < settings.health_min_insights_per_run:
            alerts.append(
                self._alert(
                    run_id=run_id,
                    alert_type="low_insight_coverage",
                    severity="warning",
                    message=(
                        f"Only {insight_count} insights were generated; "
                        f"expected at least {settings.health_min_insights_per_run}."
                    ),
                    details={
                        "insights_generated": insight_count,
                        "threshold": settings.health_min_insights_per_run,
                    },
                )
            )

        evaluation = summary.get("evaluation") or {}
        if isinstance(evaluation, dict):
            total_evaluations = int(evaluation.get("evaluations_saved") or 0)
            synthetic_evaluations = int(evaluation.get("synthetic_evaluations") or 0)
            synthetic_pct = (
                (synthetic_evaluations / total_evaluations) * 100
                if total_evaluations
                else 0.0
            )
            if synthetic_pct > settings.health_max_synthetic_evaluation_pct:
                alerts.append(
                    self._alert(
                        run_id=run_id,
                        alert_type="high_synthetic_quote_usage",
                        severity="warning",
                        message=(
                            f"{synthetic_pct:.1f}% of insight evaluations used "
                            "synthetic quote data."
                        ),
                        details={
                            "synthetic_evaluations": synthetic_evaluations,
                            "evaluations_saved": total_evaluations,
                            "threshold_pct": settings.health_max_synthetic_evaluation_pct,
                        },
                    )
                )

        simulation = summary.get("simulation") or {}
        if isinstance(simulation, dict):
            total_equity = float(simulation.get("total_equity_cad") or 0.0)
            if total_equity <= 0:
                alerts.append(
                    self._alert(
                        run_id=run_id,
                        alert_type="portfolio_equity_invalid",
                        severity="critical",
                        message="Mock portfolio equity is zero or negative.",
                        details={"total_equity_cad": total_equity},
                    )
                )

        return alerts

    def evaluate_failure(
        self,
        run_id: str,
        error: Exception,
    ) -> list[LocalHealthAlert]:
        return [
            self._alert(
                run_id=run_id,
                alert_type="pipeline_failed",
                severity="critical",
                message=f"Local pipeline failed: {error}",
                details={"error": str(error)},
            )
        ]

    def evaluate_staleness(
        self,
        latest_finished_at_utc: datetime | None,
        now_utc: datetime | None = None,
    ) -> list[LocalHealthAlert]:
        now = now_utc or datetime.now(timezone.utc)
        if latest_finished_at_utc is None:
            return [
                self._alert(
                    run_id=None,
                    alert_type="pipeline_never_finished",
                    severity="critical",
                    message="No successful local pipeline run has finished yet.",
                    details={},
                )
            ]

        if latest_finished_at_utc.tzinfo is None:
            latest_finished_at_utc = latest_finished_at_utc.replace(tzinfo=timezone.utc)

        age_hours = (now - latest_finished_at_utc).total_seconds() / 3600
        if age_hours <= settings.health_stale_run_hours:
            return []

        return [
            self._alert(
                run_id=None,
                alert_type="pipeline_stale",
                severity="critical",
                message=(
                    f"Latest successful run is {age_hours:.1f} hours old; "
                    f"threshold is {settings.health_stale_run_hours:.1f} hours."
                ),
                details={
                    "latest_finished_at_utc": latest_finished_at_utc.isoformat(),
                    "age_hours": age_hours,
                    "threshold_hours": settings.health_stale_run_hours,
                },
            )
        ]

    @staticmethod
    def _alert(
        run_id: str | None,
        alert_type: str,
        severity: str,
        message: str,
        details: dict[str, Any],
    ) -> LocalHealthAlert:
        key_run = run_id or "global"
        return LocalHealthAlert(
            alert_key=f"{key_run}:{alert_type}",
            run_id=run_id,
            severity=severity,
            alert_type=alert_type,
            message=message,
            details=details,
        )


def format_local_health_alerts(alerts: list[LocalHealthAlert]) -> str:
    if not alerts:
        return "Quicksilver local health check passed."

    lines = ["Quicksilver local health alerts", ""]
    for alert in alerts:
        lines.append(f"- {alert.severity.upper()} {alert.alert_type}: {alert.message}")
    return "\n".join(lines)


def send_local_health_alerts(alerts: list[LocalHealthAlert]) -> None:
    if not alerts:
        return

    message = format_local_health_alerts(alerts)
    logger.warning(message)
    if not settings.local_health_notifications_enabled:
        return

    _send_slack(message)
    _send_email(message)


def _send_slack(message: str) -> None:
    if not settings.slack_enabled:
        return
    if not settings.slack_webhook_url:
        raise ValueError("SLACK_ENABLED is true, but SLACK_WEBHOOK_URL is missing.")

    response = requests.post(
        settings.slack_webhook_url,
        json={"text": message},
        timeout=15,
    )
    response.raise_for_status()


def _send_email(message: str) -> None:
    if not settings.email_enabled:
        return

    recipients = [
        email.strip()
        for email in settings.alert_email_to.split(",")
        if email.strip()
    ]
    if not recipients:
        raise ValueError("EMAIL_ENABLED is true, but ALERT_EMAIL_TO is missing.")

    email_message = EmailMessage()
    email_message["Subject"] = "Quicksilver local health alert"
    email_message["From"] = settings.smtp_username
    email_message["To"] = ", ".join(recipients)
    email_message.set_content(message)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
        server.starttls()
        server.login(settings.smtp_username, settings.smtp_password)
        server.send_message(email_message)
