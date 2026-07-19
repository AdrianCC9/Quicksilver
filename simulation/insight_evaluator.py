from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from simulation.price_provider import PriceQuote, ResilientPriceProvider
from storage.local_mysql_storage import LocalMySQLStorage


@dataclass(slots=True)
class EvaluationSummary:
    evaluations_saved: int
    real_market_evaluations: int
    synthetic_evaluations: int
    directionally_correct: int
    win_rate_pct: float
    real_win_rate_pct: float
    average_forward_return_pct: float
    real_average_forward_return_pct: float


class InsightPerformanceEvaluator:
    """
    Stores mark-to-market and matured horizon performance for generated insights.

    An insight is marked as:
    - `matured` when the configured horizon date has passed.
    - `marked` when it is still inside the horizon but can be valued with the
      latest available quote.
    """

    def __init__(
        self,
        storage: LocalMySQLStorage,
        price_provider: ResilientPriceProvider | None = None,
    ) -> None:
        self.storage = storage
        self.price_provider = price_provider or ResilientPriceProvider()

    def evaluate_all(self, as_of_date: date) -> EvaluationSummary:
        insights = self.storage.fetch_dashboard_table("insights")
        if insights.empty:
            return EvaluationSummary(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)

        evaluations: list[dict[str, object]] = []
        quotes_to_store: list[dict[str, object]] = []

        for _, insight in insights.iterrows():
            insight_id = insight.get("insight_id")
            if pd.isna(insight_id):
                continue

            insight_date = pd.to_datetime(insight["insight_date"]).date()
            raw_horizon_days = insight.get("horizon_days")
            horizon_days = 5 if pd.isna(raw_horizon_days) else int(raw_horizon_days)
            evaluation_date = min(as_of_date, insight_date + timedelta(days=horizon_days))
            if evaluation_date < insight_date:
                continue

            entry_quote = self.price_provider.fetch_latest_close(
                str(insight["ticker"]),
                insight_date,
            )
            current_quote = self.price_provider.fetch_latest_close(
                str(insight["ticker"]),
                evaluation_date,
            )
            if not entry_quote or not current_quote:
                continue

            quotes_to_store.extend(
                [
                    self._quote_row(entry_quote),
                    self._quote_row(current_quote),
                ]
            )
            forward_return_pct = (
                (current_quote.close_price_usd - entry_quote.close_price_usd)
                / entry_quote.close_price_usd
            ) * 100
            direction_correct = self._direction_correct(
                str(insight["signal_label"]),
                forward_return_pct,
            )
            data_sources = sorted(
                {entry_quote.data_source, current_quote.data_source}
            )
            evaluations.append(
                {
                    "insight_id": int(insight_id),
                    "ticker": str(insight["ticker"]),
                    "insight_date": insight_date,
                    "evaluation_date": evaluation_date,
                    "evaluated_at_utc": datetime.now(timezone.utc),
                    "signal_label": str(insight["signal_label"]),
                    "recommendation": (
                        None
                        if pd.isna(insight.get("recommendation"))
                        else str(insight.get("recommendation"))
                    ),
                    "signal_score": float(insight["signal_score"]),
                    "horizon_days": horizon_days,
                    "entry_quote_date": entry_quote.quote_date,
                    "current_quote_date": current_quote.quote_date,
                    "entry_price_usd": entry_quote.close_price_usd,
                    "current_price_usd": current_quote.close_price_usd,
                    "forward_return_pct": round(forward_return_pct, 6),
                    "direction_correct": int(direction_correct),
                    "is_real_market_data": int(
                        "synthetic" not in {entry_quote.data_source, current_quote.data_source}
                    ),
                    "evaluation_status": (
                        "matured"
                        if as_of_date >= insight_date + timedelta(days=horizon_days)
                        else "marked"
                    ),
                    "data_source": ",".join(data_sources),
                }
            )

        self.storage.save_price_quotes(quotes_to_store)
        self.storage.save_insight_evaluations(evaluations)
        return self._summary(evaluations)

    @staticmethod
    def _quote_row(quote: PriceQuote) -> dict[str, object]:
        return {
            "ticker": quote.ticker,
            "quote_date": quote.quote_date,
            "close_price_usd": quote.close_price_usd,
            "data_source": quote.data_source,
        }

    @staticmethod
    def _direction_correct(signal_label: str, forward_return_pct: float) -> bool:
        if signal_label == "positive":
            return forward_return_pct > 0
        if signal_label == "negative":
            return forward_return_pct < 0
        return abs(forward_return_pct) <= 1.0

    @staticmethod
    def _summary(evaluations: list[dict[str, object]]) -> EvaluationSummary:
        if not evaluations:
            return EvaluationSummary(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)

        directionally_correct = sum(
            int(evaluation["direction_correct"])
            for evaluation in evaluations
        )
        real_evaluations = [
            evaluation
            for evaluation in evaluations
            if int(evaluation["is_real_market_data"]) == 1
        ]
        real_market_evaluations = len(real_evaluations)
        synthetic_evaluations = len(evaluations) - real_market_evaluations
        average_forward_return_pct = sum(
            float(evaluation["forward_return_pct"])
            for evaluation in evaluations
        ) / len(evaluations)
        real_directionally_correct = sum(
            int(evaluation["direction_correct"])
            for evaluation in real_evaluations
        )
        real_average_forward_return_pct = (
            sum(float(evaluation["forward_return_pct"]) for evaluation in real_evaluations)
            / real_market_evaluations
            if real_market_evaluations
            else 0.0
        )

        return EvaluationSummary(
            evaluations_saved=len(evaluations),
            real_market_evaluations=real_market_evaluations,
            synthetic_evaluations=synthetic_evaluations,
            directionally_correct=directionally_correct,
            win_rate_pct=round((directionally_correct / len(evaluations)) * 100, 4),
            real_win_rate_pct=(
                round((real_directionally_correct / real_market_evaluations) * 100, 4)
                if real_market_evaluations
                else 0.0
            ),
            average_forward_return_pct=round(average_forward_return_pct, 6),
            real_average_forward_return_pct=round(real_average_forward_return_pct, 6),
        )
