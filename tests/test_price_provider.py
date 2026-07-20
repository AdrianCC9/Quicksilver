from __future__ import annotations

from datetime import date
import logging

import requests

from simulation.price_provider import (
    AlphaVantagePriceProvider,
    CachedPriceProvider,
    PolygonPriceProvider,
    PriceQuote,
    ResilientPriceProvider,
    StooqPriceProvider,
    YahooChartPriceProvider,
)


class CountingProvider:
    def __init__(self, quote: PriceQuote | None) -> None:
        self.quote = quote
        self.calls = 0

    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote | None:
        self.calls += 1
        return self.quote


class FakeJsonResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class FakeProviderSession:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[dict] = []

    def get(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return FakeJsonResponse(self.payload)


class FakeStorage:
    def __init__(self, row: dict | None) -> None:
        self.row = row
        self.calls: list[dict] = []

    def fetch_latest_price_quote(self, **kwargs):
        self.calls.append(kwargs)
        return self.row


class FakeHttpResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError("not found", response=self)


def test_yahoo_symbol_overrides_use_usd_resolvable_symbols():
    assert YahooChartPriceProvider._yahoo_symbol("BRK.B") == "BRK-B"
    assert YahooChartPriceProvider._yahoo_symbol("FI") == "FISV"
    assert YahooChartPriceProvider._yahoo_symbol("ATD") == "ATD"
    assert YahooChartPriceProvider._yahoo_symbol("IFC") == "IFC"


def test_polygon_provider_parses_latest_close():
    payload = {
        "results": [
            {"t": 1767225600000, "c": 100.0},
            {"t": 1767312000000, "c": 101.25},
        ]
    }
    session = FakeProviderSession(payload)
    provider = PolygonPriceProvider(api_key="test-key", session=session)

    quote = provider.fetch_latest_close("AAPL", date(2026, 1, 2))

    assert quote == PriceQuote(
        ticker="AAPL",
        quote_date=date(2026, 1, 2),
        close_price_usd=101.25,
        data_source="polygon",
    )
    assert session.calls


def test_alpha_vantage_provider_parses_adjusted_close():
    payload = {
        "Time Series (Daily)": {
            "2026-01-02": {"5. adjusted close": "99.75"},
            "2026-01-03": {"5. adjusted close": "101.50"},
        }
    }
    session = FakeProviderSession(payload)
    provider = AlphaVantagePriceProvider(api_key="test-key", session=session)

    quote = provider.fetch_latest_close("MSFT", date(2026, 1, 2))

    assert quote == PriceQuote(
        ticker="MSFT",
        quote_date=date(2026, 1, 2),
        close_price_usd=99.75,
        data_source="alpha_vantage",
    )


def test_resilient_price_provider_caches_primary_quotes():
    quote = PriceQuote(
        ticker="AAPL",
        quote_date=date(2026, 1, 5),
        close_price_usd=100.0,
        data_source="fixed",
    )
    primary = CountingProvider(quote)
    provider = ResilientPriceProvider(
        primary=primary,
        secondary=CountingProvider(None),
    )

    assert provider.fetch_latest_close("AAPL", date(2026, 1, 5)) == quote
    assert provider.fetch_latest_close("AAPL", date(2026, 1, 5)) == quote
    assert primary.calls == 1


def test_cached_price_provider_returns_recent_real_storage_quote():
    storage = FakeStorage(
        {
            "ticker": "AAPL",
            "quote_date": date(2026, 1, 3),
            "close_price_usd": 123.45,
            "data_source": "yahoo_chart",
        }
    )
    provider = CachedPriceProvider(storage)

    quote = provider.fetch_latest_close("AAPL", date(2026, 1, 5))

    assert quote == PriceQuote(
        ticker="AAPL",
        quote_date=date(2026, 1, 3),
        close_price_usd=123.45,
        data_source="yahoo_chart",
    )
    assert storage.calls[0]["real_only"] is True


def test_resilient_price_provider_caches_fallback_quotes():
    primary = CountingProvider(None)
    secondary = CountingProvider(None)
    provider = ResilientPriceProvider(primary=primary, secondary=secondary)

    first_quote = provider.fetch_latest_close("MISSING", date(2026, 1, 5))
    second_quote = provider.fetch_latest_close("MISSING", date(2026, 1, 5))

    assert first_quote == second_quote
    assert first_quote.data_source == "synthetic"
    assert primary.calls == 1
    assert secondary.calls == 1


def test_resilient_price_provider_skips_external_lookup_for_synthetic_only_tickers():
    primary = CountingProvider(None)
    secondary = CountingProvider(None)
    provider = ResilientPriceProvider(primary=primary, secondary=secondary)

    quote = provider.fetch_latest_close("ATD", date(2026, 1, 5))

    assert quote.data_source == "synthetic"
    assert primary.calls == 0
    assert secondary.calls == 0


def test_yahoo_provider_suppresses_repeated_service_outage_logs(monkeypatch, caplog):
    calls = 0

    def fail_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise requests.ConnectionError("DNS unavailable")

    monkeypatch.setattr(requests, "get", fail_request)
    provider = YahooChartPriceProvider()

    with caplog.at_level(logging.WARNING):
        assert provider.fetch_latest_close("AAPL", date(2026, 1, 5)) is None
        assert provider.fetch_latest_close("MSFT", date(2026, 1, 5)) is None

    assert calls == 1
    assert caplog.text.count("Yahoo price provider unavailable") == 1


def test_stooq_provider_suppresses_repeated_service_outage_logs(monkeypatch, caplog):
    calls = 0

    def fail_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise requests.ConnectionError("DNS unavailable")

    monkeypatch.setattr(requests, "get", fail_request)
    provider = StooqPriceProvider()

    with caplog.at_level(logging.WARNING):
        assert provider.fetch_latest_close("AAPL", date(2026, 1, 5)) is None
        assert provider.fetch_latest_close("MSFT", date(2026, 1, 5)) is None

    assert calls == 1
    assert caplog.text.count("Stooq price provider unavailable") == 1


def test_stooq_provider_suppresses_repeated_missing_data_logs(monkeypatch, caplog):
    calls = 0

    def not_found_request(*args, **kwargs):
        nonlocal calls
        calls += 1
        return FakeHttpResponse(404)

    monkeypatch.setattr(requests, "get", not_found_request)
    provider = StooqPriceProvider()

    with caplog.at_level(logging.INFO):
        assert provider.fetch_latest_close("AAPL", date(2026, 1, 5)) is None
        assert provider.fetch_latest_close("MSFT", date(2026, 1, 5)) is None

    assert calls == 1
    assert caplog.text.count("Stooq price provider returned no data") == 1
