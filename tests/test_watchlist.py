from config import settings
from config.watchlist import (
    EXPANDED_EQUITY_TICKERS,
    SP500_TICKERS,
    TOP_50_EQUITY_TICKERS,
    filter_to_sp500_tickers,
    get_default_watchlist,
    get_expanded_watchlist,
)


def test_sp500_watchlist_is_the_canonical_universe():
    assert len(SP500_TICKERS) == 503
    assert len(set(SP500_TICKERS)) == 503
    assert "AAPL" in SP500_TICKERS
    assert "MSFT" in SP500_TICKERS
    assert "BRK.B" in SP500_TICKERS


def test_settings_defaults_to_sp500_watchlist():
    assert settings.default_tickers == get_default_watchlist()


def test_legacy_watchlist_names_alias_sp500_universe():
    expanded = get_expanded_watchlist()

    assert TOP_50_EQUITY_TICKERS == SP500_TICKERS
    assert EXPANDED_EQUITY_TICKERS == SP500_TICKERS
    assert expanded == list(SP500_TICKERS)
    assert len(expanded) == len(set(expanded))


def test_filter_to_sp500_tickers_drops_non_sp500_names():
    assert filter_to_sp500_tickers(["AAPL", "SHOP", "MSFT", "AAPL"]) == [
        "AAPL",
        "MSFT",
    ]
