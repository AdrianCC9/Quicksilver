from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from math import sin

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from analytics.local_dashboard_data import (
    load_local_dashboard_data as load_local_dashboard_data,
)
from config import settings
from simulation.price_provider import build_price_provider


load_dotenv()

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

PLACEHOLDER_MARKERS = (
    "replace_with",
    "replace-with",
    "your_",
    "changeme",
    "change_me",
)


def get_missing_env_vars() -> list[str]:
    return [
        name
        for name in REQUIRED_ENV_VARS
        if is_missing_or_placeholder(os.getenv(name))
    ]


def is_missing_or_placeholder(value: str | None) -> bool:
    if not value:
        return True

    normalized_value = value.strip().lower()
    return any(marker in normalized_value for marker in PLACEHOLDER_MARKERS)


def use_demo_data() -> bool:
    explicit_demo_mode = os.getenv("DASHBOARD_DEMO_MODE", "").lower() == "true"
    if explicit_demo_mode:
        return True
    return settings.storage_backend == "snowflake" and bool(get_missing_env_vars())


def use_local_data() -> bool:
    return settings.storage_backend in {"mysql", "local", "local_mysql"}


def quote_identifier(identifier: str) -> str:
    """
    Snowflake table/view names cannot be passed as normal SQL parameters,
    so we validate them before putting them into a query.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsafe Snowflake identifier: {identifier}")

    return f'"{identifier.upper()}"'


def qualified_view_name(view_name: str) -> str:
    database = quote_identifier(os.getenv("SNOWFLAKE_DATABASE", ""))
    schema = quote_identifier(os.getenv("SNOWFLAKE_SCHEMA", ""))
    view = quote_identifier(view_name)

    return f"{database}.{schema}.{view}"


@st.cache_resource
def get_snowflake_connection():
    try:
        import snowflake.connector
    except ImportError as error:
        raise RuntimeError(
            "Snowflake dashboard mode requires snowflake-connector-python. "
            "Set STORAGE_BACKEND=mysql for the local dashboard."
        ) from error

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    connection = get_snowflake_connection()

    with connection.cursor() as cursor:
        cursor.execute(query)
        dataframe = cursor.fetch_pandas_all()

    dataframe.columns = [column.lower() for column in dataframe.columns]
    return dataframe


def load_dashboard_data() -> dict[str, pd.DataFrame]:
    return {
        "latest": run_query(
            f"""
            select *
            from {qualified_view_name("latest_ticker_sentiment")}
            order by published_at_utc desc
            """
        ),
        "daily": run_query(
            f"""
            select *
            from {qualified_view_name("ticker_sentiment_daily")}
            order by sentiment_date desc, ticker
            """
        ),
        "rolling": run_query(
            f"""
            select *
            from {qualified_view_name("ticker_sentiment_rolling")}
            order by sentiment_date desc, ticker
            """
        ),
        "signals": run_query(
            f"""
            select *
            from {qualified_view_name("sentiment_signal_summary")}
            order by sentiment_date desc, ticker
            """
        ),
        "market": run_query(
            f"""
            select *
            from {qualified_view_name("market_sentiment_index")}
            order by sentiment_date desc
            """
        ),
        "audit": run_query(
            f"""
            select *
            from {qualified_view_name("pipeline_claim_audit")}
            """
        ),
    }


@st.cache_data(ttl=300)
def load_demo_dashboard_data() -> dict[str, pd.DataFrame]:
    demo_tickers = settings.default_tickers[:8]
    dates = pd.date_range(
        end=pd.Timestamp(datetime.now(timezone.utc).date()),
        periods=30,
        freq="D",
    )
    daily_rows = []

    for ticker_index, ticker in enumerate(demo_tickers):
        for day_index, sentiment_date in enumerate(dates):
            score = round(
                0.34 * sin(day_index / 4 + ticker_index / 2)
                + 0.08 * sin(day_index / 2),
                4,
            )
            headline_count = 8 + ((ticker_index * 3 + day_index) % 18)
            confidence = round(0.72 + ((ticker_index + day_index) % 12) / 100, 4)
            positive_count = int(max(score, 0) * headline_count) + headline_count // 4
            negative_count = int(max(-score, 0) * headline_count) + headline_count // 5
            neutral_count = max(headline_count - positive_count - negative_count, 0)

            daily_rows.append(
                {
                    "ticker": ticker,
                    "sentiment_date": sentiment_date.date(),
                    "headline_count": headline_count,
                    "avg_compound_score": score,
                    "avg_confidence": confidence,
                    "positive_headline_count": positive_count,
                    "neutral_headline_count": neutral_count,
                    "negative_headline_count": negative_count,
                    "avg_positive_score": round(max(score, 0) + 0.2, 4),
                    "avg_neutral_score": 0.35,
                    "avg_negative_score": round(max(-score, 0) + 0.15, 4),
                    "compound_score_sum": score * headline_count,
                    "absolute_sentiment_volume": abs(score) * headline_count,
                    "confidence_weighted_compound_score": score * confidence,
                    "source_weighted_compound_score": score * 1.05,
                    "headline_volume_weighted_sentiment_index": score
                    * (1 + headline_count / 20),
                    "first_headline_at_utc": sentiment_date,
                    "latest_headline_at_utc": sentiment_date + timedelta(hours=20),
                }
            )

    daily = pd.DataFrame(daily_rows)
    daily = daily.sort_values(["ticker", "sentiment_date"])
    grouped = daily.groupby("ticker", group_keys=False)

    rolling = daily.copy()
    rolling["rolling_7_day_avg_compound_score"] = grouped[
        "avg_compound_score"
    ].transform(lambda values: values.rolling(7, min_periods=1).mean())
    rolling["rolling_7_day_avg_headline_count"] = grouped[
        "headline_count"
    ].transform(lambda values: values.rolling(7, min_periods=1).mean())
    rolling["rolling_7_day_compound_score_stddev"] = grouped[
        "avg_compound_score"
    ].transform(lambda values: values.rolling(7, min_periods=2).std())
    rolling["rolling_7_day_compound_score_sum"] = grouped[
        "compound_score_sum"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())
    rolling["rolling_7_day_absolute_sentiment_volume"] = grouped[
        "absolute_sentiment_volume"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())
    rolling_weighted_sum = grouped["compound_score_sum"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    rolling_headline_sum = grouped["headline_count"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    rolling["rolling_7_day_volume_weighted_sentiment_index"] = (
        rolling_weighted_sum / rolling_headline_sum
    )

    signals = rolling.copy()
    signals["compound_score_zscore"] = (
        signals["avg_compound_score"] - signals["rolling_7_day_avg_compound_score"]
    ) / signals["rolling_7_day_compound_score_stddev"].replace(0, pd.NA)
    signals["compound_score_zscore"] = signals["compound_score_zscore"].fillna(0)
    signals["is_positive_sentiment_signal"] = (
        (signals["avg_compound_score"] >= settings.positive_signal_threshold)
        | (signals["compound_score_zscore"] >= 1.5)
    )
    signals["is_negative_sentiment_signal"] = (
        (signals["avg_compound_score"] <= settings.negative_signal_threshold)
        | (signals["compound_score_zscore"] <= -1.5)
    )
    signals["is_positive_zscore_anomaly"] = signals["compound_score_zscore"] >= 1.5
    signals["is_negative_zscore_anomaly"] = signals["compound_score_zscore"] <= -1.5
    signals["is_zscore_anomaly"] = signals["compound_score_zscore"].abs() >= 1.5

    market_rows = []
    for sentiment_date, group in daily.groupby("sentiment_date"):
        headline_count = group["headline_count"].sum()
        market_rows.append(
            {
                "sentiment_date": sentiment_date,
                "ticker_count": group["ticker"].nunique(),
                "headline_count": headline_count,
                "equal_weight_sentiment_index": group["avg_compound_score"].mean(),
                "volume_weighted_sentiment_index": (
                    (group["avg_compound_score"] * group["headline_count"]).sum()
                    / headline_count
                ),
                "confidence_volume_weighted_sentiment_index": (
                    (
                        group["confidence_weighted_compound_score"]
                        * group["headline_count"]
                    ).sum()
                    / headline_count
                ),
                "source_volume_weighted_sentiment_index": (
                    (
                        group["source_weighted_compound_score"]
                        * group["headline_count"]
                    ).sum()
                    / headline_count
                ),
            }
        )
    market = pd.DataFrame(market_rows)
    market["rolling_7_day_volume_weighted_sentiment_index"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=1).mean()
    market["rolling_7_day_volume_weighted_sentiment_stddev"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=2).std()

    latest_rows = []
    price_rows = []
    for ticker_offset, ticker in enumerate(demo_tickers):
        ticker_daily = daily[daily["ticker"] == ticker].iloc[-1]
        score = ticker_daily["avg_compound_score"]
        latest_rows.append(
            {
                "ticker": ticker,
                "headline": f"{ticker} demo headline drives sentiment monitoring",
                "source": "Demo Wire",
                "url": "https://example.com",
                "published_at_utc": pd.Timestamp.now(tz="UTC"),
                "sentiment_label": (
                    "positive"
                    if score > 0.1
                    else "negative"
                    if score < -0.1
                    else "neutral"
                ),
                "positive_score": max(score, 0) + 0.2,
                "neutral_score": 0.4,
                "negative_score": max(-score, 0) + 0.2,
                "compound_score": score,
                "confidence": ticker_daily["avg_confidence"],
                "headline_age_hours": 1.5,
                "source_tier": 1,
            }
        )
        price_rows.append(
            {
                "ticker": ticker,
                "quote_date": dates.max().date(),
                "close_price_usd": round(
                    90 + ticker_offset * 12.75 + abs(score) * 45,
                    2,
                ),
                "data_source": "demo",
            }
        )

    audit = pd.DataFrame(
        [
            {
                "first_scored_headline_at_utc": dates.min(),
                "latest_scored_headline_at_utc": dates.max(),
                "coverage_days": 730,
                "total_scored_headlines": 365000,
                "tracked_ticker_count": 50,
                "avg_scored_headlines_per_day": 500,
                "max_scored_headlines_in_one_day": 740,
                "days_with_500_plus_scored_headlines": 410,
                "has_50_plus_tickers": True,
                "has_2_plus_years": True,
                "has_500_plus_daily_headlines": True,
            }
        ]
    )

    return {
        "latest": pd.DataFrame(latest_rows),
        "daily": daily.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "rolling": rolling.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "signals": signals.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "market": market.sort_values("sentiment_date", ascending=False),
        "audit": audit,
        "price_quotes": pd.DataFrame(price_rows),
    }


@st.cache_data(ttl=900, show_spinner=False)
def fetch_live_price_quotes(
    tickers: tuple[str, ...],
    as_of_date_text: str,
) -> pd.DataFrame:
    expected_columns = ["ticker", "quote_date", "close_price_usd", "data_source"]
    if not tickers:
        return pd.DataFrame(columns=expected_columns)

    provider = build_price_provider()
    as_of_date = date.fromisoformat(as_of_date_text)
    rows: list[dict[str, object]] = []

    for ticker in tickers:
        try:
            quote = provider.fetch_latest_close(ticker, as_of_date)
        except Exception as error:
            rows.append(
                {
                    "ticker": ticker,
                    "quote_date": pd.NaT,
                    "close_price_usd": pd.NA,
                    "data_source": f"unavailable: {error}",
                }
            )
            continue

        if quote is None:
            continue
        rows.append(
            {
                "ticker": quote.ticker,
                "quote_date": quote.quote_date,
                "close_price_usd": quote.close_price_usd,
                "data_source": quote.data_source,
            }
        )

    return pd.DataFrame(rows, columns=expected_columns)
