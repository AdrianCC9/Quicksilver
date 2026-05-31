from config import settings
from config.watchlist import TOP_50_EQUITY_TICKERS, get_default_watchlist


def test_top_50_watchlist_has_exactly_50_unique_tickers():
    assert len(TOP_50_EQUITY_TICKERS) == 50
    assert len(set(TOP_50_EQUITY_TICKERS)) == 50


def test_settings_defaults_to_top_50_watchlist():
    assert settings.default_tickers == get_default_watchlist()
