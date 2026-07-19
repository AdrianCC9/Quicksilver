from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from io import StringIO
import hashlib
import logging
from typing import Any, Protocol

import pandas as pd
import requests

from config import settings


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PriceQuote:
    ticker: str
    quote_date: date
    close_price_usd: float
    data_source: str


class PriceProvider(Protocol):
    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        ...


SYMBOL_OVERRIDES: dict[str, str] = {
    "BRK.B": "BRK-B",
    "CNR": "CNI",
    "CP": "CP",
    "FI": "FISV",
}


class PolygonPriceProvider:
    """Fetches daily equity closes from Polygon when POLYGON_API_KEY is set."""

    def __init__(
        self,
        api_key: str | None = None,
        timeout_seconds: int = 20,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.polygon_api_key
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._missing_key_logged = False
        self._service_unavailable = False

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        if not self.api_key:
            if not self._missing_key_logged:
                logger.info("Polygon price provider disabled: POLYGON_API_KEY is missing.")
                self._missing_key_logged = True
            return None
        if self._service_unavailable:
            return None

        symbol = self._polygon_symbol(ticker)
        start_date = as_of_date - timedelta(days=45)
        url = (
            "https://api.polygon.io/v2/aggs/ticker/"
            f"{symbol}/range/1/day/{start_date:%Y-%m-%d}/{as_of_date:%Y-%m-%d}"
        )
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": "120",
            "apiKey": self.api_key,
        }

        try:
            response = self._session.get(url, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            if self._is_service_error(error):
                self._service_unavailable = True
                logger.warning(
                    "Polygon price provider unavailable for this run; "
                    "using fallback quotes where needed: %s",
                    error,
                )
            else:
                logger.warning("Polygon price fetch failed for %s: %s", ticker, error)
            return None
        except ValueError as error:
            logger.warning("Polygon price fetch failed for %s: %s", ticker, error)
            return None

        results = payload.get("results") or []
        rows: list[tuple[date, float]] = []
        for result in results:
            timestamp_ms = result.get("t")
            close_price = result.get("c")
            if timestamp_ms is None or close_price is None:
                continue
            quote_date = datetime.fromtimestamp(
                int(timestamp_ms) / 1000,
                tz=timezone.utc,
            ).date()
            if quote_date <= as_of_date:
                rows.append((quote_date, float(close_price)))

        if not rows:
            return None

        quote_date, close_price = sorted(rows, key=lambda row: row[0])[-1]
        return PriceQuote(
            ticker=ticker,
            quote_date=quote_date,
            close_price_usd=round(close_price, 4),
            data_source="polygon",
        )

    @staticmethod
    def _polygon_symbol(ticker: str) -> str:
        return SYMBOL_OVERRIDES.get(ticker.upper(), ticker.replace("-", ".").upper())

    @staticmethod
    def _is_service_error(error: requests.RequestException) -> bool:
        return isinstance(
            error,
            (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.SSLError,
            ),
        )


class AlphaVantagePriceProvider:
    """Fetches daily adjusted closes from Alpha Vantage when a key is set."""

    def __init__(
        self,
        api_key: str | None = None,
        timeout_seconds: int = 20,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.alpha_vantage_api_key
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._missing_key_logged = False
        self._service_unavailable = False

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        if not self.api_key:
            if not self._missing_key_logged:
                logger.info(
                    "Alpha Vantage price provider disabled: "
                    "ALPHA_VANTAGE_API_KEY is missing."
                )
                self._missing_key_logged = True
            return None
        if self._service_unavailable:
            return None

        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": self._alpha_vantage_symbol(ticker),
            "outputsize": "compact",
            "apikey": self.api_key,
        }

        try:
            response = self._session.get(
                "https://www.alphavantage.co/query",
                params=params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            if self._is_service_error(error):
                self._service_unavailable = True
                logger.warning(
                    "Alpha Vantage price provider unavailable for this run; "
                    "using fallback quotes where needed: %s",
                    error,
                )
            else:
                logger.warning(
                    "Alpha Vantage price fetch failed for %s: %s",
                    ticker,
                    error,
                )
            return None
        except ValueError as error:
            logger.warning(
                "Alpha Vantage price fetch failed for %s: %s",
                ticker,
                error,
            )
            return None

        time_series = payload.get("Time Series (Daily)") or {}
        rows: list[tuple[date, float]] = []
        for quote_date_text, values in time_series.items():
            try:
                quote_date = datetime.strptime(quote_date_text, "%Y-%m-%d").date()
                close_value = values.get("5. adjusted close") or values.get("4. close")
                if close_value is None or quote_date > as_of_date:
                    continue
                rows.append((quote_date, float(close_value)))
            except (TypeError, ValueError):
                continue

        if not rows:
            return None

        quote_date, close_price = sorted(rows, key=lambda row: row[0])[-1]
        return PriceQuote(
            ticker=ticker,
            quote_date=quote_date,
            close_price_usd=round(close_price, 4),
            data_source="alpha_vantage",
        )

    @staticmethod
    def _alpha_vantage_symbol(ticker: str) -> str:
        return SYMBOL_OVERRIDES.get(ticker.upper(), ticker.replace("-", ".").upper())

    @staticmethod
    def _is_service_error(error: requests.RequestException) -> bool:
        return isinstance(
            error,
            (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.SSLError,
            ),
        )


class CachedPriceProvider:
    """Uses recent real quotes already persisted in local storage."""

    def __init__(
        self,
        storage: Any,
        max_age_days: int = 14,
    ) -> None:
        self.storage = storage
        self.max_age_days = max_age_days

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        row = self.storage.fetch_latest_price_quote(
            ticker=ticker,
            as_of_date=as_of_date,
            max_age_days=self.max_age_days,
            real_only=True,
        )
        if not row:
            return None

        return PriceQuote(
            ticker=ticker,
            quote_date=row["quote_date"],
            close_price_usd=float(row["close_price_usd"]),
            data_source=str(row["data_source"]),
        )


class YahooChartPriceProvider:
    """Fetches daily equity closes from Yahoo's public chart endpoint."""

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self._service_unavailable = False

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        if self._service_unavailable:
            return None

        start_date = as_of_date - timedelta(days=45)
        yahoo_symbol = self._yahoo_symbol(ticker)
        period1 = self._epoch_seconds(start_date)
        period2 = self._epoch_seconds(as_of_date + timedelta(days=1))
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{yahoo_symbol}?period1={period1}&period2={period2}&interval=1d"
        )

        try:
            response = requests.get(
                url,
                timeout=self.timeout_seconds,
                headers={"User-Agent": "Mozilla/5.0 Quicksilver/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            if self._is_service_error(error):
                self._service_unavailable = True
                logger.warning(
                    "Yahoo price provider unavailable for this run; "
                    "using fallback quotes where needed: %s",
                    error,
                )
            else:
                logger.warning("Yahoo price fetch failed for %s: %s", ticker, error)
            return None
        except ValueError as error:
            logger.warning("Yahoo price fetch failed for %s: %s", ticker, error)
            return None

        try:
            result = payload["chart"]["result"][0]
            timestamps = result["timestamp"]
            close_prices = result["indicators"]["quote"][0]["close"]
        except (KeyError, IndexError, TypeError):
            return None

        rows = [
            (
                datetime.fromtimestamp(timestamp, tz=timezone.utc).date(),
                float(close_price),
            )
            for timestamp, close_price in zip(timestamps, close_prices)
            if close_price is not None
        ]
        rows = [
            (quote_date, close_price)
            for quote_date, close_price in rows
            if quote_date <= as_of_date
        ]
        if not rows:
            return None

        quote_date, close_price = sorted(rows, key=lambda row: row[0])[-1]
        return PriceQuote(
            ticker=ticker,
            quote_date=quote_date,
            close_price_usd=round(close_price, 4),
            data_source="yahoo_chart",
        )

    @classmethod
    def _yahoo_symbol(cls, ticker: str) -> str:
        return SYMBOL_OVERRIDES.get(ticker.upper(), ticker.replace(".", "-").upper())

    @staticmethod
    def _epoch_seconds(value: date) -> int:
        return int(datetime.combine(value, time.min, tzinfo=timezone.utc).timestamp())

    @staticmethod
    def _is_service_error(error: requests.RequestException) -> bool:
        return isinstance(
            error,
            (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.SSLError,
            ),
        )


class StooqPriceProvider:
    """Fetches daily US equity closes from Stooq without API credentials."""

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self._service_unavailable = False
        self._missing_data_logged = False

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote | None:
        if self._service_unavailable:
            return None

        stooq_symbol = self._stooq_symbol(ticker)
        start_date = as_of_date - timedelta(days=10)
        url = (
            "https://stooq.com/q/d/l/"
            f"?s={stooq_symbol}&d1={start_date:%Y%m%d}&d2={as_of_date:%Y%m%d}&i=d"
        )

        try:
            response = requests.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as error:
            if self._is_service_error(error):
                self._service_unavailable = True
                logger.warning(
                    "Stooq price provider unavailable for this run; "
                    "using fallback quotes where needed: %s",
                    error,
                )
            elif self._is_missing_data_error(error):
                self._service_unavailable = True
                if not self._missing_data_logged:
                    logger.info(
                        "Stooq price provider returned no data for the requested "
                        "range; using cached or synthetic fallback quotes for this run."
                    )
                    self._missing_data_logged = True
            else:
                logger.warning("Price fetch failed for %s: %s", ticker, error)
            return None

        dataframe = pd.read_csv(StringIO(response.text))
        if dataframe.empty or "Close" not in dataframe.columns:
            return None

        dataframe["Date"] = pd.to_datetime(dataframe["Date"], errors="coerce")
        dataframe = dataframe.dropna(subset=["Date", "Close"])
        if dataframe.empty:
            return None

        latest = dataframe.sort_values("Date").iloc[-1]
        return PriceQuote(
            ticker=ticker,
            quote_date=latest["Date"].date(),
            close_price_usd=float(latest["Close"]),
            data_source="stooq",
        )

    @staticmethod
    def _stooq_symbol(ticker: str) -> str:
        return ticker.lower().replace(".", "-") + ".us"

    @staticmethod
    def _is_service_error(error: requests.RequestException) -> bool:
        return isinstance(
            error,
            (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.SSLError,
            ),
        )

    @staticmethod
    def _is_missing_data_error(error: requests.RequestException) -> bool:
        response = getattr(error, "response", None)
        return response is not None and response.status_code == 404


class SyntheticPriceProvider:
    """
    Deterministic fallback prices for offline tests and first-run demos.

    The prices drift by date so the portfolio can still produce a visible
    equity curve when live quote lookup is unavailable.
    """

    def fetch_latest_close(
        self,
        ticker: str,
        as_of_date: date,
    ) -> PriceQuote:
        digest = hashlib.sha256(f"{ticker}:{as_of_date.isoformat()}".encode()).hexdigest()
        base = 40 + (int(digest[:6], 16) % 360)
        drift = ((int(digest[6:10], 16) % 900) - 450) / 1000
        price = max(5.0, base * (1 + drift / 10))
        return PriceQuote(
            ticker=ticker,
            quote_date=as_of_date,
            close_price_usd=round(price, 2),
            data_source="synthetic",
        )


class ResilientPriceProvider:
    SYNTHETIC_ONLY_TICKERS = {"ATD", "IFC"}

    def __init__(
        self,
        primary: PriceProvider | None = None,
        secondary: PriceProvider | None = None,
        providers: list[PriceProvider] | None = None,
        fallback: SyntheticPriceProvider | None = None,
    ) -> None:
        if providers is None:
            providers = [
                primary or YahooChartPriceProvider(),
                secondary or StooqPriceProvider(),
            ]
        self.providers = providers
        self.primary = providers[0] if providers else None
        self.secondary = providers[1] if len(providers) > 1 else None
        self.fallback = fallback or SyntheticPriceProvider()
        self._cache: dict[tuple[str, date], PriceQuote] = {}

    def fetch_latest_close(self, ticker: str, as_of_date: date) -> PriceQuote:
        cache_key = (ticker.upper(), as_of_date)
        cached_quote = self._cache.get(cache_key)
        if cached_quote is not None:
            return cached_quote

        if cache_key[0] in self.SYNTHETIC_ONLY_TICKERS:
            quote = self.fallback.fetch_latest_close(ticker, as_of_date)
            self._cache[cache_key] = quote
            return quote

        for provider in self.providers:
            quote = provider.fetch_latest_close(ticker, as_of_date)
            if quote is not None:
                self._cache[cache_key] = quote
                return quote

        quote = self.fallback.fetch_latest_close(ticker, as_of_date)
        self._cache[cache_key] = quote
        return quote


def build_price_provider(storage: Any | None = None) -> ResilientPriceProvider:
    providers: list[PriceProvider] = []
    provider_order = [
        provider.strip().lower()
        for provider in settings.price_provider_order.split(",")
        if provider.strip()
    ]
    if not provider_order:
        provider_order = ["polygon", "alpha_vantage", "yahoo", "stooq"]

    for provider_name in provider_order:
        if provider_name == "polygon":
            if settings.polygon_api_key:
                providers.append(PolygonPriceProvider())
        elif provider_name in {"alpha_vantage", "alphavantage"}:
            if settings.alpha_vantage_api_key:
                providers.append(AlphaVantagePriceProvider())
        elif provider_name in {"yahoo", "yahoo_chart"}:
            providers.append(YahooChartPriceProvider())
        elif provider_name == "stooq":
            providers.append(StooqPriceProvider())
        elif provider_name == "synthetic":
            continue
        else:
            logger.warning("Unknown price provider %r, skipping.", provider_name)

    if storage is not None:
        providers.append(CachedPriceProvider(storage))
    if not providers:
        providers.extend([YahooChartPriceProvider(), StooqPriceProvider()])

    return ResilientPriceProvider(providers=providers)
