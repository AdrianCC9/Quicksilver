from __future__ import annotations

from datetime import date, datetime, timezone

from models.insight import Insight
from simulation.insight_evaluator import InsightPerformanceEvaluator
from simulation.price_provider import PriceQuote
from storage.local_mysql_storage import LocalMySQLStorage


class RisingPriceProvider:
    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote:
        return PriceQuote(
            ticker=ticker,
            quote_date=as_of_date,
            close_price_usd=100.0 if as_of_date == date(2026, 1, 1) else 110.0,
            data_source="yahoo_chart",
        )


def test_insight_evaluator_stores_directional_real_market_performance(tmp_path):
    storage = LocalMySQLStorage(f"sqlite:///{tmp_path / 'eval.db'}")
    storage.create_tables()
    storage.save_insights(
        [
            Insight(
                ticker="AAPL",
                insight_date=date(2026, 1, 1),
                generated_at_utc=datetime.now(timezone.utc),
                signal_label="positive",
                signal_score=0.5,
                confidence=0.8,
                headline_count=3,
                political_headline_count=0,
                financial_headline_count=3,
                category_mix="financial:3/3",
                rationale="Positive test insight.",
                recommendation="buy",
                horizon_days=5,
            )
        ]
    )

    summary = InsightPerformanceEvaluator(
        storage=storage,
        price_provider=RisingPriceProvider(),
    ).evaluate_all(as_of_date=date(2026, 1, 6))

    evaluations = storage.fetch_dashboard_table("insight_evaluations")
    assert summary.evaluations_saved == 1
    assert summary.win_rate_pct == 100
    assert len(evaluations) == 1
    assert evaluations.iloc[0]["forward_return_pct"] == 10
    assert evaluations.iloc[0]["direction_correct"] == 1
    assert evaluations.iloc[0]["is_real_market_data"] == 1
    storage.close()

