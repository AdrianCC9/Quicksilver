import os
import re
import sys
from datetime import datetime, timedelta
from math import sin
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings


load_dotenv()
sns.set_theme(style="whitegrid")

st.set_page_config(
    page_title="Quicksilver Dashboard",
    page_icon="QS",
    layout="wide",
)


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
    return explicit_demo_mode or bool(get_missing_env_vars())


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
        end=pd.Timestamp(datetime.utcnow().date()),
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
    rolling["rolling_7_day_volume_weighted_sentiment_index"] = grouped.apply(
        lambda group: (
            (group["avg_compound_score"] * group["headline_count"])
            .rolling(7, min_periods=1)
            .sum()
            / group["headline_count"].rolling(7, min_periods=1).sum()
        )
    ).reset_index(level=0, drop=True)
    rolling["rolling_7_day_compound_score_sum"] = grouped[
        "compound_score_sum"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())
    rolling["rolling_7_day_absolute_sentiment_volume"] = grouped[
        "absolute_sentiment_volume"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())

    signals = rolling.copy()
    signals["compound_score_zscore"] = (
        signals["avg_compound_score"] - signals["rolling_7_day_avg_compound_score"]
    ) / signals["rolling_7_day_compound_score_stddev"].replace(0, pd.NA)
    signals["compound_score_zscore"] = signals["compound_score_zscore"].fillna(0)
    signals["is_positive_sentiment_signal"] = (
        (signals["avg_compound_score"] >= 0.25)
        | (signals["compound_score_zscore"] >= 1.5)
    )
    signals["is_negative_sentiment_signal"] = (
        (signals["avg_compound_score"] <= -0.25)
        | (signals["compound_score_zscore"] <= -1.5)
    )
    signals["is_positive_zscore_anomaly"] = signals["compound_score_zscore"] >= 1.5
    signals["is_negative_zscore_anomaly"] = signals["compound_score_zscore"] <= -1.5
    signals["is_zscore_anomaly"] = signals["compound_score_zscore"].abs() >= 1.5

    market = (
        daily.groupby("sentiment_date")
        .apply(
            lambda group: pd.Series(
                {
                    "ticker_count": group["ticker"].nunique(),
                    "headline_count": group["headline_count"].sum(),
                    "equal_weight_sentiment_index": group[
                        "avg_compound_score"
                    ].mean(),
                    "volume_weighted_sentiment_index": (
                        group["avg_compound_score"] * group["headline_count"]
                    ).sum()
                    / group["headline_count"].sum(),
                    "confidence_volume_weighted_sentiment_index": (
                        group["confidence_weighted_compound_score"]
                        * group["headline_count"]
                    ).sum()
                    / group["headline_count"].sum(),
                    "source_volume_weighted_sentiment_index": (
                        group["source_weighted_compound_score"]
                        * group["headline_count"]
                    ).sum()
                    / group["headline_count"].sum(),
                }
            )
        )
        .reset_index()
    )
    market["rolling_7_day_volume_weighted_sentiment_index"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=1).mean()
    market["rolling_7_day_volume_weighted_sentiment_stddev"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=2).std()

    latest_rows = []
    for ticker in demo_tickers:
        ticker_daily = daily[daily["ticker"] == ticker].iloc[-1]
        score = ticker_daily["avg_compound_score"]
        latest_rows.append(
            {
                "ticker": ticker,
                "headline": f"{ticker} demo headline drives sentiment monitoring",
                "source": "Demo Wire",
                "url": "https://example.com",
                "published_at_utc": pd.Timestamp.utcnow(),
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
    }


def filter_by_ticker(dataframe: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    if dataframe.empty or not tickers or "ticker" not in dataframe.columns:
        return dataframe

    return dataframe[dataframe["ticker"].isin(tickers)]


def build_sentiment_chart(dataframe: pd.DataFrame):
    chart_data = dataframe.copy()

    if chart_data.empty:
        return None

    chart_data["sentiment_date"] = pd.to_datetime(chart_data["sentiment_date"])
    chart_data = chart_data.sort_values("sentiment_date")

    fig, ax = plt.subplots(figsize=(13, 5))
    sns.lineplot(
        data=chart_data,
        x="sentiment_date",
        y="rolling_7_day_volume_weighted_sentiment_index",
        hue="ticker",
        marker="o",
        linewidth=1.8,
        ax=ax,
    )
    ax.axhline(0, color="#666666", linewidth=1, alpha=0.6)
    ax.set_title("Ticker Sentiment Trend")
    ax.set_xlabel("Date")
    ax.set_ylabel("7-day volume-weighted sentiment")
    ax.legend(title="Ticker", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    return fig


def build_market_index_chart(dataframe: pd.DataFrame):
    chart_data = dataframe.copy()

    if chart_data.empty:
        return None

    chart_data["sentiment_date"] = pd.to_datetime(chart_data["sentiment_date"])
    chart_data = chart_data.sort_values("sentiment_date")

    fig, ax = plt.subplots(figsize=(13, 4))
    sns.lineplot(
        data=chart_data,
        x="sentiment_date",
        y="volume_weighted_sentiment_index",
        marker="o",
        label="Daily volume-weighted index",
        ax=ax,
    )
    sns.lineplot(
        data=chart_data,
        x="sentiment_date",
        y="rolling_7_day_volume_weighted_sentiment_index",
        marker="o",
        label="7-day rolling index",
        ax=ax,
    )
    ax.axhline(0, color="#666666", linewidth=1, alpha=0.6)
    ax.set_title("Market Sentiment Index")
    ax.set_xlabel("Date")
    ax.set_ylabel("Sentiment index")
    fig.tight_layout()
    return fig


def display_metric_cards(
    daily: pd.DataFrame,
    signals: pd.DataFrame,
    market: pd.DataFrame,
    audit: pd.DataFrame,
) -> None:
    if daily.empty:
        st.info("No daily sentiment data available yet.")
        return

    latest_date = daily["sentiment_date"].max()
    latest_daily = daily[daily["sentiment_date"] == latest_date]

    latest_signals = signals
    if not signals.empty:
        latest_signal_date = signals["sentiment_date"].max()
        latest_signals = signals[signals["sentiment_date"] == latest_signal_date]

    headline_count = int(latest_daily["headline_count"].sum())
    avg_sentiment = latest_daily["avg_compound_score"].mean()

    positive_signals = 0
    negative_signals = 0

    if not latest_signals.empty:
        positive_signals = int(latest_signals["is_positive_sentiment_signal"].sum())
        negative_signals = int(latest_signals["is_negative_sentiment_signal"].sum())

    market_index = None
    if not market.empty:
        latest_market_date = market["sentiment_date"].max()
        latest_market = market[market["sentiment_date"] == latest_market_date]
        market_index = latest_market["volume_weighted_sentiment_index"].iloc[0]

    tracked_ticker_count = len(latest_daily["ticker"].unique())
    coverage_days = None
    if not audit.empty and "coverage_days" in audit.columns:
        coverage_days = audit["coverage_days"].iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Latest data date", str(latest_date))
    col2.metric("Headlines scored", f"{headline_count:,}")
    col3.metric("Tracked tickers", f"{tracked_ticker_count}")
    col4.metric(
        "Market index",
        "n/a" if pd.isna(market_index) else f"{market_index:.3f}",
        help="Headline-volume-weighted sentiment across the tracked universe.",
    )
    col5.metric(
        "Active signals",
        f"{positive_signals + negative_signals}",
        delta=f"+{positive_signals} / -{negative_signals}",
    )

    if coverage_days is not None and not pd.isna(coverage_days):
        st.caption(f"Warehouse coverage: {int(coverage_days):,} days")


def main() -> None:
    st.title("Quicksilver Operational Dashboard")

    with st.sidebar:
        st.header("Controls")
        demo_mode = use_demo_data()

        if demo_mode:
            st.info("Demo data mode")

        if st.button("Refresh data"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    if demo_mode:
        missing_env_vars = get_missing_env_vars()
        if missing_env_vars:
            st.info(
                "Using generated demo data because Snowflake credentials are "
                "missing or still placeholders."
            )
        data = load_demo_dashboard_data()
    else:
        try:
            data = load_dashboard_data()
        except Exception as error:
            st.error("Could not load dashboard data from Snowflake.")
            st.exception(error)
            return

    latest = data["latest"]
    daily = data["daily"]
    rolling = data["rolling"]
    signals = data["signals"]
    market = data["market"]
    audit = data["audit"]

    all_tickers = sorted(
        set(latest.get("ticker", pd.Series(dtype=str)).dropna().tolist())
        | set(daily.get("ticker", pd.Series(dtype=str)).dropna().tolist())
    )

    with st.sidebar:
        selected_tickers = st.multiselect(
            "Tickers",
            options=all_tickers,
            default=all_tickers,
        )

    latest = filter_by_ticker(latest, selected_tickers)
    daily = filter_by_ticker(daily, selected_tickers)
    rolling = filter_by_ticker(rolling, selected_tickers)
    signals = filter_by_ticker(signals, selected_tickers)

    display_metric_cards(daily, signals, market, audit)

    st.subheader("Sentiment Trend")
    chart = build_sentiment_chart(rolling)

    if chart is None:
        st.info("No rolling sentiment data available yet.")
    else:
        st.pyplot(chart, width="stretch")

    st.subheader("Market Sentiment Index")
    market_chart = build_market_index_chart(market)

    if market_chart is None:
        st.info("No market sentiment index data available yet.")
    else:
        st.pyplot(market_chart, width="stretch")

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Latest Ticker Sentiment")
        st.dataframe(
            latest[
                [
                    "ticker",
                    "sentiment_label",
                    "compound_score",
                    "confidence",
                    "source",
                    "published_at_utc",
                    "headline",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with right_col:
        st.subheader("Signal Summary")
        if signals.empty:
            st.info("No signal data available yet.")
        else:
            signal_columns = [
                "ticker",
                "sentiment_date",
                "headline_count",
                "avg_compound_score",
                "rolling_7_day_avg_compound_score",
                "rolling_7_day_volume_weighted_sentiment_index",
                "compound_score_zscore",
                "is_positive_sentiment_signal",
                "is_negative_sentiment_signal",
                "is_zscore_anomaly",
            ]

            st.dataframe(
                signals[signal_columns],
                width="stretch",
                hide_index=True,
            )

    st.subheader("Daily Sentiment Detail")
    st.dataframe(
        daily,
        width="stretch",
        hide_index=True,
    )

    st.subheader("Pipeline Claim Audit")
    if audit.empty:
        st.info("No claim audit data available yet.")
    else:
        st.dataframe(
            audit,
            width="stretch",
            hide_index=True,
        )


if __name__ == "__main__":
    main()
