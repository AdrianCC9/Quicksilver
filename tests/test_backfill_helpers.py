from datetime import date
from types import SimpleNamespace

from pipelines.backfill_historical_headlines import date_windows, resolve_tickers


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


def test_resolve_tickers_uses_large_cap_50_watchlist():
    args = SimpleNamespace(
        large_cap_50=True,
        ticker_file=None,
        tickers=None,
    )

    assert len(resolve_tickers(args)) == 50
