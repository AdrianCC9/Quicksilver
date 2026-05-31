"""
Canonical equity watchlist for the Quicksilver pipeline.

The list is intentionally hardcoded so the streaming, backfill, dbt, dashboard,
and resume story all reference the same 50-ticker universe by default.
"""

TOP_50_EQUITY_TICKERS: tuple[str, ...] = (
    "NVDA",
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "AVGO",
    "GOOG",
    "META",
    "TSLA",
    "BRK.B",
    "JPM",
    "LLY",
    "V",
    "NFLX",
    "MA",
    "XOM",
    "WMT",
    "COST",
    "JNJ",
    "PG",
    "HD",
    "ABBV",
    "BAC",
    "ORCL",
    "KO",
    "PM",
    "GE",
    "CSCO",
    "CRM",
    "CVX",
    "WFC",
    "ABT",
    "IBM",
    "MCD",
    "AMD",
    "LIN",
    "MRK",
    "TMO",
    "DIS",
    "PEP",
    "NOW",
    "ACN",
    "ISRG",
    "QCOM",
    "INTU",
    "UBER",
    "VZ",
    "TXN",
    "CAT",
    "AMGN",
)


def get_default_watchlist() -> list[str]:
    return list(TOP_50_EQUITY_TICKERS)
