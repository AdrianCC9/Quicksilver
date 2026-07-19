from __future__ import annotations

from datetime import date

import pandas as pd

from simulation.mock_exchange import MockExchange
from simulation.price_provider import PriceQuote
from storage.local_mysql_storage import LocalMySQLStorage


class FixedPriceProvider:
    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote:
        return PriceQuote(
            ticker=ticker,
            quote_date=as_of_date,
            close_price_usd=100.0 if ticker == "AAPL" else 50.0,
            data_source="fixed",
        )


class SyntheticPriceProvider:
    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote:
        return PriceQuote(
            ticker=ticker,
            quote_date=as_of_date,
            close_price_usd=100.0,
            data_source="synthetic",
        )


def test_mock_exchange_buys_positive_insight_with_cad_cash(tmp_path):
    storage = LocalMySQLStorage(f"sqlite:///{tmp_path / 'exchange.db'}")
    storage.create_tables()
    insights = pd.DataFrame(
        [
            {
                "insight_id": 1,
                "ticker": "AAPL",
                "insight_date": date.today(),
                "signal_label": "positive",
                "signal_score": 0.5,
                "confidence": 0.9,
                "rationale": "Positive signal.",
            }
        ]
    )

    result = MockExchange(
        storage=storage,
        price_provider=FixedPriceProvider(),
        usd_to_cad_rate=1.25,
    ).rebalance_from_insights(
        insights=insights,
        as_of_date=date.today(),
        run_name="test",
        starting_cash_cad=5000,
        max_positions=3,
        cash_reserve_pct=0.05,
    )

    assert result.trades_executed == 1
    assert result.total_equity_cad == 5000
    assert len(storage.fetch_dashboard_table("portfolio_trades")) == 1
    assert len(storage.fetch_dashboard_table("portfolio_positions")) == 1
    assert len(storage.fetch_dashboard_table("portfolio_snapshots")) == 1
    storage.close()


def test_mock_exchange_does_not_trade_on_synthetic_quotes(tmp_path):
    storage = LocalMySQLStorage(f"sqlite:///{tmp_path / 'exchange.db'}")
    storage.create_tables()
    insights = pd.DataFrame(
        [
            {
                "insight_id": 1,
                "ticker": "AAPL",
                "insight_date": date.today(),
                "signal_label": "positive",
                "signal_score": 0.5,
                "confidence": 0.9,
                "rationale": "Positive signal.",
            }
        ]
    )

    result = MockExchange(
        storage=storage,
        price_provider=SyntheticPriceProvider(),
        usd_to_cad_rate=1.25,
    ).rebalance_from_insights(
        insights=insights,
        as_of_date=date.today(),
        run_name="test",
        starting_cash_cad=5000,
        max_positions=3,
        cash_reserve_pct=0.05,
    )

    assert result.trades_executed == 0
    assert result.cash_cad == 5000
    assert result.total_equity_cad == 5000
    assert storage.fetch_dashboard_table("portfolio_trades").empty
    assert storage.fetch_dashboard_table("portfolio_positions").empty
    storage.close()
