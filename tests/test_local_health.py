from __future__ import annotations

from alerts.local_health import LocalPipelineHealthMonitor


def test_health_monitor_flags_low_coverage_and_synthetic_usage(monkeypatch):
    monkeypatch.setattr("config.settings.health_min_raw_headlines_per_run", 10)
    monkeypatch.setattr("config.settings.health_min_insights_per_run", 3)
    monkeypatch.setattr("config.settings.health_max_synthetic_evaluation_pct", 25)

    alerts = LocalPipelineHealthMonitor().evaluate_success(
        {
            "run_id": "run-1",
            "raw_headlines_collected": 2,
            "insights_generated": 1,
            "evaluation": {
                "evaluations_saved": 4,
                "synthetic_evaluations": 2,
            },
            "simulation": {
                "total_equity_cad": 5000,
            },
        }
    )

    assert {alert.alert_type for alert in alerts} == {
        "low_headline_coverage",
        "low_insight_coverage",
        "high_synthetic_quote_usage",
    }


def test_health_monitor_flags_failures():
    alerts = LocalPipelineHealthMonitor().evaluate_failure(
        "run-2",
        RuntimeError("boom"),
    )

    assert len(alerts) == 1
    assert alerts[0].severity == "critical"
    assert alerts[0].alert_type == "pipeline_failed"
