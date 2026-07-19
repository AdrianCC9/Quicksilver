from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from config import settings
from simulation.price_provider import PriceQuote, ResilientPriceProvider
from storage.local_mysql_storage import LocalMySQLStorage


@dataclass(slots=True)
class SimulationResult:
    run_name: str
    cash_cad: float
    positions_value_cad: float
    total_equity_cad: float
    cumulative_return_pct: float
    trades_executed: int


class MockExchange:
    def __init__(
        self,
        storage: LocalMySQLStorage,
        price_provider: ResilientPriceProvider | None = None,
        usd_to_cad_rate: float | None = None,
    ) -> None:
        self.storage = storage
        self.price_provider = price_provider or ResilientPriceProvider()
        self.usd_to_cad_rate = usd_to_cad_rate or settings.usd_to_cad_rate

    def rebalance_from_insights(
        self,
        insights: pd.DataFrame,
        as_of_date: date,
        run_name: str,
        starting_cash_cad: float,
        max_positions: int,
        cash_reserve_pct: float,
    ) -> SimulationResult:
        self.storage.create_tables()
        run = self.storage.get_or_create_portfolio_run(run_name, starting_cash_cad)
        run_id = int(run["portfolio_run_id"])
        cash_cad = float(run["cash_cad"])
        starting_cash = float(run["starting_cash_cad"])

        latest_insights = self._latest_insights_by_ticker(insights)
        positions = self.storage.fetch_positions(run_id)
        current_tickers = set(positions["ticker"].tolist()) if not positions.empty else set()
        candidate_tickers = set(latest_insights["ticker"].tolist()) | current_tickers
        quotes = self._fetch_quotes(candidate_tickers, as_of_date)
        self.storage.save_price_quotes(
            [
                {
                    "ticker": quote.ticker,
                    "quote_date": quote.quote_date,
                    "close_price_usd": quote.close_price_usd,
                    "data_source": quote.data_source,
                }
                for quote in quotes.values()
            ]
        )

        positions = self._mark_positions(run_id, positions, quotes)
        positions_value = float(positions["market_value_cad"].sum()) if not positions.empty else 0.0
        total_equity = cash_cad + positions_value

        trades_executed = 0
        negative_tickers = set(
            latest_insights[latest_insights["signal_label"] == "negative"]["ticker"].tolist()
        )
        for _, position in positions.iterrows():
            ticker = position["ticker"]
            quote = quotes.get(ticker)
            if ticker in negative_tickers and self._is_tradeable_quote(quote):
                cash_cad += self._sell_position(
                    run_id=run_id,
                    position=position,
                    quote=quote,
                    reason="Liquidated after negative insight signal.",
                )
                trades_executed += 1

        positive = latest_insights[
            (latest_insights["signal_label"] == "positive")
            & (latest_insights["signal_score"] >= settings.positive_signal_threshold)
        ].copy()
        if "recommendation" in positive.columns:
            positive = positive[
                positive["recommendation"].fillna("buy").isin(
                    ["strong_buy", "buy", "watch"]
                )
            ]
        if "risk_score" in positive.columns:
            positive = positive[positive["risk_score"].fillna(0.0) <= 0.65]
        positive = positive.sort_values(
            [
                "opportunity_score" if "opportunity_score" in positive.columns else "signal_score",
                "signal_score",
                "confidence",
            ],
            ascending=[False, False, False],
        ).head(max_positions)

        positions = self.storage.fetch_positions(run_id)
        positions_value = float(positions["market_value_cad"].sum()) if not positions.empty else 0.0
        total_equity = cash_cad + positions_value
        investable_equity = total_equity * max(0.0, min(1.0, 1 - cash_reserve_pct))

        if not positive.empty:
            weights = self._allocation_weights(positive)
            existing_by_ticker = (
                positions.set_index("ticker").to_dict("index")
                if not positions.empty
                else {}
            )

            for _, insight in positive.iterrows():
                ticker = insight["ticker"]
                quote = quotes.get(ticker)
                if not self._is_tradeable_quote(quote):
                    continue

                price_cad = quote.close_price_usd * self.usd_to_cad_rate
                current_value = float(existing_by_ticker.get(ticker, {}).get("market_value_cad", 0.0))
                target_value = investable_equity * weights.get(ticker, 0.0)
                amount_to_buy = min(max(target_value - current_value, 0.0), cash_cad)
                if amount_to_buy < 25:
                    continue

                quantity = amount_to_buy / price_cad
                current_quantity = float(existing_by_ticker.get(ticker, {}).get("quantity", 0.0))
                current_avg_cost = float(existing_by_ticker.get(ticker, {}).get("avg_cost_cad", 0.0))
                new_quantity = current_quantity + quantity
                new_avg_cost = (
                    ((current_quantity * current_avg_cost) + amount_to_buy) / new_quantity
                    if new_quantity
                    else price_cad
                )

                cash_cad -= amount_to_buy
                self.storage.upsert_position(
                    portfolio_run_id=run_id,
                    ticker=ticker,
                    quantity=new_quantity,
                    avg_cost_cad=new_avg_cost,
                    last_price_cad=price_cad,
                )
                self.storage.insert_trade(
                    portfolio_run_id=run_id,
                    insight_id=self._nullable_int(insight.get("insight_id")),
                    ticker=ticker,
                    side="buy",
                    quantity=quantity,
                    price_cad=price_cad,
                    gross_cad=amount_to_buy,
                    reason=str(insight.get("rationale") or "Positive insight signal."),
                )
                trades_executed += 1

        self.storage.update_portfolio_cash(run_id, cash_cad)
        final_positions = self._mark_positions(
            run_id,
            self.storage.fetch_positions(run_id),
            quotes,
        )
        final_positions_value = (
            float(final_positions["market_value_cad"].sum())
            if not final_positions.empty
            else 0.0
        )
        total_equity = cash_cad + final_positions_value
        cumulative_return_pct = (
            ((total_equity - starting_cash) / starting_cash) * 100
            if starting_cash
            else 0.0
        )
        data_source = self._combined_data_source(quotes)
        self.storage.save_snapshot(
            portfolio_run_id=run_id,
            snapshot_date=as_of_date,
            cash_cad=cash_cad,
            positions_value_cad=final_positions_value,
            total_equity_cad=total_equity,
            starting_cash_cad=starting_cash,
            data_source=data_source,
        )

        return SimulationResult(
            run_name=run_name,
            cash_cad=round(cash_cad, 2),
            positions_value_cad=round(final_positions_value, 2),
            total_equity_cad=round(total_equity, 2),
            cumulative_return_pct=round(cumulative_return_pct, 4),
            trades_executed=trades_executed,
        )

    @staticmethod
    def _latest_insights_by_ticker(insights: pd.DataFrame) -> pd.DataFrame:
        if insights.empty:
            return insights

        working = insights.copy()
        working["insight_date"] = pd.to_datetime(working["insight_date"]).dt.date
        return (
            working.sort_values(["insight_date", "signal_score"], ascending=[False, False])
            .groupby("ticker", as_index=False)
            .head(1)
        )

    def _fetch_quotes(
        self,
        tickers: set[str],
        as_of_date: date,
    ) -> dict[str, PriceQuote]:
        quotes: dict[str, PriceQuote] = {}
        for ticker in sorted(tickers):
            quotes[ticker] = self.price_provider.fetch_latest_close(ticker, as_of_date)
        return quotes

    @staticmethod
    def _allocation_weights(insights: pd.DataFrame) -> dict[str, float]:
        if insights.empty:
            return {}

        raw_weights: dict[str, float] = {}
        for _, insight in insights.iterrows():
            ticker = str(insight["ticker"])
            opportunity = (
                float(insight.get("opportunity_score"))
                if "opportunity_score" in insights.columns and not pd.isna(insight.get("opportunity_score"))
                else max(float(insight.get("signal_score", 0.0)), 0.0)
            )
            confidence = (
                float(insight.get("confidence"))
                if not pd.isna(insight.get("confidence"))
                else 0.5
            )
            recommendation = str(insight.get("recommendation") or "buy")
            recommendation_multiplier = {
                "strong_buy": 1.35,
                "buy": 1.0,
                "watch": 0.45,
            }.get(recommendation, 0.25)
            raw_weights[ticker] = max(opportunity * confidence * recommendation_multiplier, 0.01)

        total = sum(raw_weights.values())
        return {
            ticker: weight / total
            for ticker, weight in raw_weights.items()
        }

    def _mark_positions(
        self,
        run_id: int,
        positions: pd.DataFrame,
        quotes: dict[str, PriceQuote],
    ) -> pd.DataFrame:
        if positions.empty:
            return positions

        marked = positions.copy()
        for index, position in marked.iterrows():
            ticker = position["ticker"]
            quote = quotes.get(ticker)
            if not self._is_markable_quote(quote):
                continue

            price_cad = quote.close_price_usd * self.usd_to_cad_rate
            self.storage.upsert_position(
                portfolio_run_id=run_id,
                ticker=ticker,
                quantity=float(position["quantity"]),
                avg_cost_cad=float(position["avg_cost_cad"]),
                last_price_cad=price_cad,
            )
            market_value = float(position["quantity"]) * price_cad
            marked.loc[index, "last_price_cad"] = price_cad
            marked.loc[index, "market_value_cad"] = market_value
            marked.loc[index, "unrealized_pnl_cad"] = market_value - (
                float(position["quantity"]) * float(position["avg_cost_cad"])
            )

        return marked

    def _sell_position(
        self,
        run_id: int,
        position: pd.Series,
        quote: PriceQuote,
        reason: str,
    ) -> float:
        price_cad = quote.close_price_usd * self.usd_to_cad_rate
        quantity = float(position["quantity"])
        gross = quantity * price_cad
        self.storage.insert_trade(
            portfolio_run_id=run_id,
            ticker=str(position["ticker"]),
            side="sell",
            quantity=quantity,
            price_cad=price_cad,
            gross_cad=gross,
            reason=reason,
        )
        self.storage.upsert_position(
            portfolio_run_id=run_id,
            ticker=str(position["ticker"]),
            quantity=0.0,
            avg_cost_cad=0.0,
            last_price_cad=price_cad,
        )
        return gross

    @staticmethod
    def _nullable_int(value: Any) -> int | None:
        if pd.isna(value):
            return None
        return int(value)

    @staticmethod
    def _is_tradeable_quote(quote: PriceQuote | None) -> bool:
        return quote is not None and quote.data_source != "synthetic"

    @staticmethod
    def _is_markable_quote(quote: PriceQuote | None) -> bool:
        return quote is not None and quote.data_source != "synthetic"

    @staticmethod
    def _combined_data_source(quotes: dict[str, PriceQuote]) -> str:
        sources = sorted({quote.data_source for quote in quotes.values()})
        return ",".join(sources) if sources else "none"
