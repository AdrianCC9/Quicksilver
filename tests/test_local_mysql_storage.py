from __future__ import annotations

from datetime import date, datetime, timezone

from models.insight import Insight
from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline
from storage.local_mysql_storage import LocalMySQLStorage


def test_local_storage_dedupes_headlines_and_upserts_insights(tmp_path):
    storage = LocalMySQLStorage(f"sqlite:///{tmp_path / 'quicksilver.db'}")
    storage.create_tables()

    raw = RawHeadline(
        ticker="AAPL",
        headline="Apple beats expectations",
        source="Reuters",
        url="https://example.com/aapl",
        published_at_utc=datetime.now(timezone.utc),
    )
    storage.save_raw_headlines([raw, raw])

    scored = ScoredHeadline(
        ticker=raw.ticker,
        headline=raw.headline,
        source=raw.source,
        url=raw.url,
        published_at_utc=raw.published_at_utc,
        sentiment_label="positive",
        positive_score=0.8,
        neutral_score=0.1,
        negative_score=0.1,
        compound_score=0.7,
        confidence=0.8,
        headline_age_hours=1.0,
        source_tier=1,
    )
    storage.save_scored_headlines([scored, scored])

    recent = storage.fetch_recent_scored_headlines(
        since_utc=datetime(2020, 1, 1, tzinfo=timezone.utc)
    )
    assert len(storage.fetch_dashboard_table("raw_headlines")) == 1
    assert len(recent) == 1

    insight = Insight(
        ticker="AAPL",
        insight_date=date.today(),
        generated_at_utc=datetime.now(timezone.utc),
        signal_label="positive",
        signal_score=0.5,
        confidence=0.8,
        headline_count=1,
        political_headline_count=0,
        financial_headline_count=1,
        category_mix="financial:1/1",
        rationale="Positive test signal.",
        source_headline_hashes=["abc"],
    )
    storage.save_insights([insight])
    insight.signal_score = 0.6
    storage.save_insights([insight])

    insights = storage.fetch_dashboard_table("insights")
    assert len(insights) == 1
    assert insights.iloc[0]["signal_score"] == 0.6

    storage.save_health_alerts(
        [
            {
                "alert_key": "run-1:low_coverage",
                "run_id": "run-1",
                "severity": "warning",
                "alert_type": "low_coverage",
                "message": "Coverage is low.",
                "details": {"raw_headlines": 2},
            }
        ]
    )
    storage.resolve_health_alerts(["low_coverage"])
    resolved_alerts = storage.fetch_dashboard_table("health_alerts")
    assert resolved_alerts.iloc[0]["status"] == "resolved"

    storage.save_report_run(
        report_name="weekly_performance",
        period_start=date.today(),
        period_end=date.today(),
        output_dir=".data/reports/test",
        markdown_path=".data/reports/test/report.md",
        pdf_path=".data/reports/test/report.pdf",
        metrics={"evaluated_insights": 1},
    )

    assert len(storage.fetch_dashboard_table("health_alerts")) == 1
    assert len(storage.fetch_dashboard_table("report_runs")) == 1

    storage.save_price_quotes(
        [
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 1),
                "close_price_usd": 100.0,
                "data_source": "yahoo_chart",
            },
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 2),
                "close_price_usd": 200.0,
                "data_source": "synthetic",
            },
        ]
    )
    cached_quote = storage.fetch_latest_price_quote(
        ticker="AAPL",
        as_of_date=date(2026, 1, 3),
        real_only=True,
    )
    assert cached_quote is not None
    assert cached_quote["quote_date"] == date(2026, 1, 1)
    assert cached_quote["close_price_usd"] == 100.0

    storage.close()
