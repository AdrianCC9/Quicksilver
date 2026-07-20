from __future__ import annotations

from datetime import date

import pandas as pd

from analytics.local_dashboard_data import _build_performance_summary
from dashboard.app import (
    build_hot_stocks,
    build_portfolio_stock_frame,
    build_watchlist_price_frame,
    default_selected_tickers,
    filter_open_health_alerts,
    latest_price_quotes,
    portfolio_summary,
    sentiment_breakdown_for_ticker,
    trade_summary_for_ticker,
)


def test_default_selected_tickers_prefers_latest_high_volume_names():
    daily = pd.DataFrame(
        [
            {"ticker": "AAPL", "sentiment_date": "2026-01-01", "headline_count": 2},
            {"ticker": "MSFT", "sentiment_date": "2026-01-02", "headline_count": 4},
            {"ticker": "NVDA", "sentiment_date": "2026-01-02", "headline_count": 9},
            {"ticker": "TSLA", "sentiment_date": "2026-01-02", "headline_count": 1},
        ]
    )

    assert default_selected_tickers(
        daily,
        ["AAPL", "MSFT", "NVDA", "TSLA"],
        limit=3,
    ) == ["NVDA", "MSFT", "TSLA"]


def test_performance_summary_separates_real_and_synthetic_evaluations():
    evaluations = pd.DataFrame(
        [
            {
                "direction_correct": 1,
                "is_real_market_data": 1,
                "forward_return_pct": 2.0,
                "evaluation_status": "marked",
                "signal_label": "positive",
                "recommendation": "buy",
            },
            {
                "direction_correct": 0,
                "is_real_market_data": 0,
                "forward_return_pct": -50.0,
                "evaluation_status": "marked",
                "signal_label": "positive",
                "recommendation": "buy",
            },
        ]
    )

    overall = _build_performance_summary(evaluations)
    overall = overall[overall["segment"] == "overall"].iloc[0]

    assert overall["evaluated_insights"] == 2
    assert overall["real_market_evaluations"] == 1
    assert overall["synthetic_evaluations"] == 1
    assert overall["win_rate_pct"] == 50
    assert overall["real_win_rate_pct"] == 100
    assert overall["real_avg_forward_return_pct"] == 2


def test_filter_open_health_alerts_keeps_only_active_alerts():
    health_alerts = pd.DataFrame(
        [
            {"alert_type": "high_synthetic_usage", "status": "resolved"},
            {"alert_type": "stale_pipeline", "status": "open"},
            {"alert_type": "missing_report", "status": "OPEN"},
        ]
    )

    open_alerts = filter_open_health_alerts(health_alerts)

    assert open_alerts["alert_type"].tolist() == [
        "stale_pipeline",
        "missing_report",
    ]


def test_latest_price_quotes_returns_most_recent_quote_per_ticker():
    price_quotes = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 1),
                "close_price_usd": 100.0,
                "data_source": "yahoo_chart",
            },
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 3),
                "close_price_usd": 102.5,
                "data_source": "stooq",
            },
            {
                "ticker": "MSFT",
                "quote_date": date(2026, 1, 2),
                "close_price_usd": 300.0,
                "data_source": "yahoo_chart",
            },
        ]
    )

    latest_quotes = latest_price_quotes(price_quotes)

    assert latest_quotes.set_index("ticker").loc["AAPL", "close_price_usd"] == 102.5
    assert latest_quotes.set_index("ticker").loc["MSFT", "data_source"] == "yahoo_chart"


def test_watchlist_prices_prefer_real_cached_quote_over_synthetic_live_quote():
    cached_quotes = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 2),
                "close_price_usd": 100.0,
                "data_source": "yahoo_chart",
            }
        ]
    )
    live_quotes = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "quote_date": date(2026, 1, 5),
                "close_price_usd": 500.0,
                "data_source": "synthetic",
            },
            {
                "ticker": "MSFT",
                "quote_date": date(2026, 1, 5),
                "close_price_usd": 410.0,
                "data_source": "yahoo_chart",
            },
        ]
    )

    prices = build_watchlist_price_frame(
        ["AAPL", "MSFT"],
        cached_quotes,
        live_quotes,
    ).set_index("ticker")

    assert prices.loc["AAPL", "close_price_usd"] == 100.0
    assert prices.loc["AAPL", "data_source"] == "yahoo_chart"
    assert prices.loc["MSFT", "close_price_usd"] == 410.0


def test_hot_stocks_uses_latest_signal_date_and_headline_context():
    signals = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "sentiment_date": date(2026, 1, 1),
                "headline_count": 50,
                "avg_compound_score": 0.2,
                "signal_score": 0.2,
            },
            {
                "ticker": "AAPL",
                "sentiment_date": date(2026, 1, 2),
                "headline_count": 2,
                "political_headline_count": 0,
                "signal_score": 0.1,
                "opportunity_score": 0.1,
                "signal_label": "neutral",
            },
            {
                "ticker": "MSFT",
                "sentiment_date": date(2026, 1, 2),
                "headline_count": 12,
                "political_headline_count": 2,
                "signal_score": 0.4,
                "opportunity_score": 0.5,
                "signal_label": "positive",
                "rationale": "Strong cloud demand across current headlines.",
            },
        ]
    )
    latest = pd.DataFrame(
        [
            {
                "ticker": "MSFT",
                "headline": "Microsoft shares rise on cloud demand",
                "source": "Demo Wire",
                "published_at_utc": "2026-01-02T12:00:00Z",
            }
        ]
    )

    hot = build_hot_stocks(signals, latest, limit=1)

    assert hot.iloc[0]["ticker"] == "MSFT"
    assert hot.iloc[0]["latest_headline"] == "Microsoft shares rise on cloud demand"
    assert "cloud demand" in hot.iloc[0]["why_hot"]


def test_portfolio_summary_reflects_5000_cad_budget_and_profit():
    runs = pd.DataFrame(
        [
            {
                "run_name": "default",
                "starting_cash_cad": 5000.0,
                "updated_at_utc": "2026-01-01T12:00:00Z",
            }
        ]
    )
    snapshots = pd.DataFrame(
        [
            {
                "snapshot_date": date(2026, 1, 2),
                "cash_cad": 400.0,
                "positions_value_cad": 5050.0,
                "total_equity_cad": 5450.0,
                "cumulative_return_pct": 9.0,
                "data_source": "yahoo_chart",
            }
        ]
    )

    summary = portfolio_summary(runs, snapshots)

    assert summary["starting_cash_cad"] == 5000.0
    assert summary["total_equity_cad"] == 5450.0
    assert summary["profit_cad"] == 450.0
    assert summary["return_pct"] == 9.0


def test_stock_drilldown_summarizes_sentiment_trades_and_returns():
    daily = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "sentiment_date": date(2026, 1, 2),
                "headline_count": 10,
                "positive_headline_count": 6,
                "neutral_headline_count": 3,
                "negative_headline_count": 1,
                "avg_compound_score": 0.24,
            }
        ]
    )
    positions = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "quantity": 2.0,
                "avg_cost_cad": 100.0,
                "last_price_cad": 125.0,
                "market_value_cad": 250.0,
                "unrealized_pnl_cad": 50.0,
            }
        ]
    )
    trades = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "side": "buy",
                "quantity": 2.0,
                "price_cad": 100.0,
                "gross_cad": 200.0,
            }
        ]
    )
    evaluations = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "direction_correct": 1,
                "is_real_market_data": 1,
                "forward_return_pct": 4.5,
            }
        ]
    )
    prices = pd.DataFrame()

    sentiment = sentiment_breakdown_for_ticker(daily, "AAPL")
    trade_summary = trade_summary_for_ticker("AAPL", positions, trades, prices)
    stock_frame = build_portfolio_stock_frame(
        ["AAPL"],
        positions,
        trades,
        daily,
        evaluations,
        prices,
    ).set_index("ticker")

    assert sentiment["positive_pct"] == 60
    assert trade_summary["bought_quantity"] == 2.0
    assert trade_summary["avg_buy_price_cad"] == 100.0
    assert trade_summary["open_return_pct"] == 25.0
    assert stock_frame.loc["AAPL", "model_win_rate_pct"] == 100.0
