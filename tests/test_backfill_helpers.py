from datetime import date, datetime, timezone
from types import SimpleNamespace

from pipelines.backfill_historical_headlines import (
    build_parser,
    date_windows,
    resolve_tickers,
    run_historical_recommendation_backtest,
)
from models.scored_headline import ScoredHeadline
from simulation.price_provider import PriceQuote
from storage.local_mysql_storage import LocalMySQLStorage


def test_date_windows_chunks_inclusive_ranges():
    windows = list(
        date_windows(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 10),
            chunk_days=4,
        )
    )

    assert windows == [
        (date(2026, 1, 1), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 8)),
        (date(2026, 1, 9), date(2026, 1, 10)),
    ]


def test_resolve_tickers_uses_sp500_alias_watchlist():
    args = SimpleNamespace(
        large_cap_50=True,
        ticker_file=None,
        tickers=None,
    )

    tickers = resolve_tickers(args)

    assert len(tickers) == 503
    assert "AAPL" in tickers


def test_backfill_defaults_to_about_six_months():
    args = build_parser().parse_args([])

    assert (args.to_date - args.from_date).days == 183
    assert args.skip_backtest is False


class FixedPriceProvider:
    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote:
        return PriceQuote(
            ticker=ticker,
            quote_date=as_of_date,
            close_price_usd=100.0,
            data_source="fixed",
        )


def test_historical_backfill_can_create_recommendations_and_backtest(
    tmp_path,
    monkeypatch,
):
    database_url = f"sqlite:///{tmp_path / 'backfill.db'}"
    storage = LocalMySQLStorage(database_url)
    storage.create_tables()
    storage.save_scored_headlines(
        [
            ScoredHeadline(
                ticker="AAPL",
                headline="Apple beats expectations",
                source="Demo Wire",
                url="https://example.com/aapl",
                published_at_utc=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
                sentiment_label="positive",
                positive_score=0.8,
                neutral_score=0.1,
                negative_score=0.1,
                compound_score=0.7,
                confidence=0.8,
                headline_age_hours=1.0,
                source_tier=2,
            )
        ]
    )
    monkeypatch.setattr(
        "pipelines.backfill_historical_headlines.build_price_provider",
        lambda storage: FixedPriceProvider(),
    )

    summary = run_historical_recommendation_backtest(
        storage=storage,
        tickers=["AAPL"],
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
    )

    assert summary["insights_generated"] == 1
    assert summary["backtest_days"] == 1
    assert summary["evaluations_saved"] == 1
    assert len(storage.fetch_dashboard_table("portfolio_snapshots")) == 1
    storage.close()
