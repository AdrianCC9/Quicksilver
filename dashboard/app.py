from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_sources import (
    fetch_live_price_quotes,
    load_dashboard_data,
    load_demo_dashboard_data,
    load_local_dashboard_data,
    use_demo_data,
    use_local_data,
)
from config import settings
from config.news_topics import TICKER_COMPANY_NAMES, get_sector_for_ticker


DEFAULT_TICKER_SELECTION_LIMIT = 12

st.set_page_config(
    page_title="Quicksilver",
    page_icon="QS",
    layout="wide",
)


def apply_student_theme() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #ffffff;
            color: #111827;
        }
        h1, h2, h3 {
            color: #111827;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 0;
            border-left: 4px solid #15803d;
            border-radius: 0;
            padding-left: 0.8rem;
        }
        button,
        input,
        textarea,
        [data-baseweb="select"] > div,
        [data-baseweb="tag"],
        div[data-testid="stDataFrame"] {
            border-radius: 0 !important;
        }
        div[data-testid="stDataFrame"] > div,
        div[data-testid="stTable"] > div,
        div[data-testid="stExpander"] details,
        div[data-testid="stAlert"] {
            border-radius: 0 !important;
        }
        hr {
            border-color: #15803d;
        }
        button[kind="primary"],
        div[data-testid="stTabs"] button[aria-selected="true"] {
            color: #15803d !important;
        }
        div[data-testid="stTabs"] button {
            border-radius: 0 !important;
        }
        section[data-testid="stSidebar"] {
            background: #ffffff;
            border-right: 1px solid #d1d5db;
        }
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: #15803d;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def filter_by_ticker(dataframe: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    if dataframe.empty or not tickers or "ticker" not in dataframe.columns:
        return dataframe
    return dataframe[dataframe["ticker"].astype(str).str.upper().isin(tickers)]


def default_selected_tickers(
    daily: pd.DataFrame,
    all_tickers: list[str],
    limit: int = DEFAULT_TICKER_SELECTION_LIMIT,
) -> list[str]:
    if not all_tickers:
        return []
    if daily.empty or "headline_count" not in daily.columns:
        return all_tickers[:limit]

    latest_date = daily["sentiment_date"].max()
    latest_daily = daily[daily["sentiment_date"] == latest_date]
    ranked_tickers = (
        latest_daily.sort_values("headline_count", ascending=False)["ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .drop_duplicates()
        .tolist()
    )
    return list(dict.fromkeys(ranked_tickers + all_tickers))[:limit]


def select_existing_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing_columns = [column for column in columns if column in dataframe.columns]
    if not existing_columns:
        return pd.DataFrame()
    return dataframe[existing_columns]


def filter_open_health_alerts(health_alerts: pd.DataFrame) -> pd.DataFrame:
    if health_alerts.empty or "status" not in health_alerts.columns:
        return health_alerts
    return health_alerts[
        health_alerts["status"].fillna("").astype(str).str.lower() == "open"
    ]


def latest_price_quotes(price_quotes: pd.DataFrame) -> pd.DataFrame:
    expected_columns = ["ticker", "quote_date", "close_price_usd", "data_source"]
    if price_quotes.empty:
        return pd.DataFrame(columns=expected_columns)

    quotes = select_existing_columns(price_quotes, expected_columns).copy()
    if quotes.empty or "ticker" not in quotes.columns:
        return pd.DataFrame(columns=expected_columns)
    for column in expected_columns:
        if column not in quotes.columns:
            quotes[column] = pd.NA

    quotes["ticker"] = quotes["ticker"].astype(str).str.upper()
    quotes["quote_timestamp"] = pd.to_datetime(quotes["quote_date"], errors="coerce")
    quotes["quote_date"] = quotes["quote_timestamp"].dt.date
    quotes["close_price_usd"] = pd.to_numeric(quotes["close_price_usd"], errors="coerce")
    quotes = quotes.sort_values(
        ["ticker", "quote_timestamp"],
        ascending=[True, False],
        na_position="last",
    )
    return quotes.groupby("ticker", as_index=False).head(1)[expected_columns]


def combine_price_quotes(
    cached_quotes: pd.DataFrame,
    live_quotes: pd.DataFrame,
) -> pd.DataFrame:
    expected_columns = ["ticker", "quote_date", "close_price_usd", "data_source"]
    frames = [
        select_existing_columns(frame, expected_columns)
        for frame in (live_quotes, cached_quotes)
        if not frame.empty
    ]
    if not frames:
        return pd.DataFrame(columns=expected_columns)

    combined = pd.concat(frames, ignore_index=True)
    for column in expected_columns:
        if column not in combined.columns:
            combined[column] = pd.NA
    combined["ticker"] = combined["ticker"].astype(str).str.upper()
    combined["quote_timestamp"] = pd.to_datetime(combined["quote_date"], errors="coerce")
    combined["quote_date"] = combined["quote_timestamp"].dt.date
    combined["close_price_usd"] = pd.to_numeric(
        combined["close_price_usd"],
        errors="coerce",
    )
    source = combined["data_source"].fillna("").astype(str).str.lower()
    combined["is_missing_price"] = combined["close_price_usd"].isna()
    combined["is_synthetic"] = source.str.contains("synthetic|demo|unavailable")
    combined = combined.sort_values(
        ["ticker", "is_missing_price", "is_synthetic", "quote_timestamp"],
        ascending=[True, True, True, False],
        na_position="last",
    )
    return combined.groupby("ticker", as_index=False).head(1)[expected_columns]


def build_watchlist_price_frame(
    tickers: list[str],
    cached_quotes: pd.DataFrame,
    live_quotes: pd.DataFrame,
) -> pd.DataFrame:
    ticker_list = [
        ticker.strip().upper()
        for ticker in tickers
        if ticker and ticker.strip()
    ]
    ticker_list = list(dict.fromkeys(ticker_list))
    base = pd.DataFrame(
        [
            {
                "ticker": ticker,
                "company": TICKER_COMPANY_NAMES.get(ticker, ticker),
                "sector": get_sector_for_ticker(ticker),
            }
            for ticker in ticker_list
        ]
    )
    if base.empty:
        return base

    quotes = combine_price_quotes(cached_quotes, live_quotes)
    if quotes.empty:
        base["quote_date"] = pd.NaT
        base["close_price_usd"] = pd.NA
        base["data_source"] = pd.NA
    else:
        base = base.merge(quotes, on="ticker", how="left")

    return base[
        ["ticker", "company", "sector", "close_price_usd", "quote_date", "data_source"]
    ]


def build_hot_stocks(
    signals: pd.DataFrame,
    latest: pd.DataFrame,
    limit: int = 8,
) -> pd.DataFrame:
    if signals.empty and latest.empty:
        return pd.DataFrame()

    if signals.empty:
        working = latest.copy()
        working["headline_count"] = 1
        working["avg_compound_score"] = numeric_column(working, "compound_score")
        working["signal_score"] = working["avg_compound_score"]
        working["signal_label"] = working["avg_compound_score"].apply(_signal_from_score)
    else:
        working = signals.copy()
        if "sentiment_date" in working.columns:
            working["sentiment_date"] = pd.to_datetime(
                working["sentiment_date"],
                errors="coerce",
            ).dt.date
            latest_signal_date = working["sentiment_date"].max()
            working = working[working["sentiment_date"] == latest_signal_date]

    latest_headlines = _latest_headline_lookup(latest)
    if not latest_headlines.empty:
        working = working.merge(latest_headlines, on="ticker", how="left")

    score_column = (
        "signal_score"
        if "signal_score" in working.columns
        else "avg_compound_score"
        if "avg_compound_score" in working.columns
        else "compound_score"
    )
    working["hot_signal_score"] = numeric_column(working, score_column)
    working["headline_count"] = numeric_column(working, "headline_count")
    working["political_headline_count"] = numeric_column(
        working,
        "political_headline_count",
    )
    working["compound_score_zscore"] = numeric_column(working, "compound_score_zscore")
    working["opportunity_score"] = numeric_column(working, "opportunity_score")

    if "signal_label" not in working.columns:
        working["signal_label"] = working["hot_signal_score"].apply(_signal_from_score)

    working["news_heat_score"] = (
        working["headline_count"] * (0.55 + working["hot_signal_score"].abs())
        + working["political_headline_count"] * 0.75
        + working["compound_score_zscore"].abs() * 2
        + working["opportunity_score"] * 3
    ).round(2)
    working["why_hot"] = ""
    if "rationale" in working.columns:
        working["why_hot"] = working["rationale"].fillna("").astype(str)
    if "latest_headline" in working.columns:
        empty_reason = working["why_hot"].str.strip() == ""
        working.loc[empty_reason, "why_hot"] = (
            working.loc[empty_reason, "latest_headline"].fillna("").astype(str)
        )

    display_columns = [
        "ticker",
        "news_heat_score",
        "signal_label",
        "headline_count",
        "political_headline_count",
        "hot_signal_score",
        "recommendation",
        "confidence_grade",
        "latest_source",
        "latest_headline",
        "why_hot",
    ]
    return (
        select_existing_columns(
            working.sort_values("news_heat_score", ascending=False),
            display_columns,
        )
        .head(limit)
        .reset_index(drop=True)
    )


def portfolio_summary(
    portfolio_runs: pd.DataFrame,
    portfolio_snapshots: pd.DataFrame,
) -> dict[str, float | str | None]:
    starting_cash = settings.portfolio_initial_cash_cad
    if not portfolio_runs.empty and "starting_cash_cad" in portfolio_runs.columns:
        run_sort_column = (
            "updated_at_utc"
            if "updated_at_utc" in portfolio_runs.columns
            else "created_at_utc"
            if "created_at_utc" in portfolio_runs.columns
            else None
        )
        latest_run = (
            portfolio_runs.sort_values(run_sort_column).iloc[-1]
            if run_sort_column
            else portfolio_runs.iloc[-1]
        )
        starting_cash = safe_float(latest_run.get("starting_cash_cad"), starting_cash)

    if portfolio_snapshots.empty:
        return {
            "starting_cash_cad": starting_cash,
            "cash_cad": starting_cash,
            "positions_value_cad": 0.0,
            "total_equity_cad": starting_cash,
            "profit_cad": 0.0,
            "return_pct": 0.0,
            "snapshot_date": None,
            "data_source": "none",
        }

    snapshots = portfolio_snapshots.copy()
    if "snapshot_date" in snapshots.columns:
        snapshots["snapshot_date"] = pd.to_datetime(
            snapshots["snapshot_date"],
            errors="coerce",
        )
        snapshots = snapshots.sort_values("snapshot_date")
    latest_snapshot = snapshots.iloc[-1]
    total_equity = safe_float(latest_snapshot.get("total_equity_cad"), starting_cash)
    profit = total_equity - starting_cash
    return_pct = (
        safe_float(latest_snapshot.get("cumulative_return_pct"))
        if "cumulative_return_pct" in latest_snapshot.index
        else (profit / starting_cash) * 100
        if starting_cash
        else 0.0
    )
    snapshot_date = latest_snapshot.get("snapshot_date")
    if hasattr(snapshot_date, "date"):
        snapshot_date = snapshot_date.date()

    return {
        "starting_cash_cad": starting_cash,
        "cash_cad": safe_float(latest_snapshot.get("cash_cad")),
        "positions_value_cad": safe_float(latest_snapshot.get("positions_value_cad")),
        "total_equity_cad": total_equity,
        "profit_cad": profit,
        "return_pct": return_pct,
        "snapshot_date": snapshot_date,
        "data_source": str(latest_snapshot.get("data_source", "n/a")),
    }


def evaluation_summary(evaluations: pd.DataFrame, ticker: str | None = None) -> dict[str, float]:
    empty = {
        "evaluated": 0.0,
        "real_evaluated": 0.0,
        "win_rate_pct": 0.0,
        "real_win_rate_pct": 0.0,
        "avg_forward_return_pct": 0.0,
    }
    if evaluations.empty:
        return empty

    working = evaluations.copy()
    if ticker and "ticker" in working.columns:
        working = working[working["ticker"].astype(str).str.upper() == ticker.upper()]
    if working.empty:
        return empty

    direction_correct = numeric_column(working, "direction_correct")
    real_mask = numeric_column(working, "is_real_market_data").astype(int) == 1
    real_working = working[real_mask]
    real_direction_correct = direction_correct[real_mask]
    return {
        "evaluated": float(len(working)),
        "real_evaluated": float(len(real_working)),
        "win_rate_pct": float(direction_correct.mean() * 100) if len(working) else 0.0,
        "real_win_rate_pct": (
            float(real_direction_correct.mean() * 100)
            if len(real_working)
            else 0.0
        ),
        "avg_forward_return_pct": float(numeric_column(working, "forward_return_pct").mean()),
    }


def sentiment_breakdown_for_ticker(
    daily: pd.DataFrame,
    ticker: str,
) -> dict[str, float]:
    empty_breakdown = {
        "positive_count": 0.0,
        "neutral_count": 0.0,
        "negative_count": 0.0,
        "headline_count": 0.0,
        "positive_pct": 0.0,
        "neutral_pct": 0.0,
        "negative_pct": 0.0,
        "avg_compound_score": 0.0,
    }
    if daily.empty or "ticker" not in daily.columns:
        return empty_breakdown

    ticker_daily = daily[daily["ticker"].astype(str).str.upper() == ticker.upper()].copy()
    if ticker_daily.empty:
        return empty_breakdown
    if "sentiment_date" in ticker_daily.columns:
        ticker_daily["sentiment_date"] = pd.to_datetime(
            ticker_daily["sentiment_date"],
            errors="coerce",
        )
        ticker_daily = ticker_daily.sort_values("sentiment_date")
    latest_row = ticker_daily.iloc[-1]

    positive = safe_float(latest_row.get("positive_headline_count"))
    neutral = safe_float(latest_row.get("neutral_headline_count"))
    negative = safe_float(latest_row.get("negative_headline_count"))
    total = positive + neutral + negative
    if total <= 0:
        total = safe_float(latest_row.get("headline_count"))

    return {
        "positive_count": positive,
        "neutral_count": neutral,
        "negative_count": negative,
        "headline_count": total,
        "positive_pct": (positive / total) * 100 if total else 0.0,
        "neutral_pct": (neutral / total) * 100 if total else 0.0,
        "negative_pct": (negative / total) * 100 if total else 0.0,
        "avg_compound_score": safe_float(latest_row.get("avg_compound_score")),
    }


def trade_summary_for_ticker(
    ticker: str,
    portfolio_positions: pd.DataFrame,
    portfolio_trades: pd.DataFrame,
    watchlist_prices: pd.DataFrame,
) -> dict[str, float | str]:
    ticker = ticker.upper()
    positions = pd.DataFrame()
    if not portfolio_positions.empty and "ticker" in portfolio_positions.columns:
        positions = portfolio_positions[
            portfolio_positions["ticker"].astype(str).str.upper() == ticker
        ].copy()

    trades = pd.DataFrame()
    if not portfolio_trades.empty and "ticker" in portfolio_trades.columns:
        trades = portfolio_trades[
            portfolio_trades["ticker"].astype(str).str.upper() == ticker
        ].copy()

    if not trades.empty and "side" in trades.columns:
        trades["side"] = trades["side"].fillna("").astype(str).str.lower()
    buy_trades = trades[trades["side"] == "buy"] if "side" in trades.columns else pd.DataFrame()
    sell_trades = trades[trades["side"] == "sell"] if "side" in trades.columns else pd.DataFrame()

    bought_quantity = numeric_column(buy_trades, "quantity").sum() if not buy_trades.empty else 0.0
    sold_quantity = numeric_column(sell_trades, "quantity").sum() if not sell_trades.empty else 0.0
    buy_gross = numeric_column(buy_trades, "gross_cad").sum() if not buy_trades.empty else 0.0
    if bought_quantity and buy_gross:
        avg_buy_price = buy_gross / bought_quantity
    elif bought_quantity and not buy_trades.empty:
        avg_buy_price = (
            numeric_column(buy_trades, "price_cad")
            * numeric_column(buy_trades, "quantity")
        ).sum() / bought_quantity
    else:
        avg_buy_price = 0.0

    current_quantity = numeric_column(positions, "quantity").sum() if not positions.empty else 0.0
    market_value = numeric_column(positions, "market_value_cad").sum() if not positions.empty else 0.0
    unrealized_pnl = numeric_column(positions, "unrealized_pnl_cad").sum() if not positions.empty else 0.0
    last_price = 0.0
    if not positions.empty and current_quantity:
        last_price = (
            numeric_column(positions, "last_price_cad")
            * numeric_column(positions, "quantity")
        ).sum() / current_quantity
    elif not watchlist_prices.empty and "ticker" in watchlist_prices.columns:
        quote_rows = watchlist_prices[
            watchlist_prices["ticker"].astype(str).str.upper() == ticker
        ]
        if not quote_rows.empty:
            last_price = (
                safe_float(quote_rows.iloc[0].get("close_price_usd"))
                * settings.usd_to_cad_rate
            )

    open_cost = current_quantity * avg_buy_price
    open_return_pct = (unrealized_pnl / open_cost) * 100 if open_cost else 0.0
    latest_trade_at = ""
    if not trades.empty and "traded_at_utc" in trades.columns:
        latest_trade_at = str(pd.to_datetime(trades["traded_at_utc"], errors="coerce").max())

    return {
        "bought_quantity": float(bought_quantity),
        "sold_quantity": float(sold_quantity),
        "avg_buy_price_cad": float(avg_buy_price),
        "current_quantity": float(current_quantity),
        "last_price_cad": float(last_price),
        "market_value_cad": float(market_value),
        "unrealized_pnl_cad": float(unrealized_pnl),
        "open_return_pct": float(open_return_pct),
        "latest_trade_at": latest_trade_at,
    }


def build_portfolio_stock_frame(
    tickers: list[str],
    portfolio_positions: pd.DataFrame,
    portfolio_trades: pd.DataFrame,
    daily: pd.DataFrame,
    insight_evaluations: pd.DataFrame,
    watchlist_prices: pd.DataFrame,
) -> pd.DataFrame:
    available_tickers = list(tickers)
    for dataframe in (portfolio_positions, portfolio_trades, daily, insight_evaluations):
        if not dataframe.empty and "ticker" in dataframe.columns:
            available_tickers.extend(
                dataframe["ticker"].dropna().astype(str).str.upper().tolist()
            )
    unique_tickers = list(dict.fromkeys(available_tickers))

    rows = []
    for ticker in unique_tickers:
        trade_summary = trade_summary_for_ticker(
            ticker,
            portfolio_positions,
            portfolio_trades,
            watchlist_prices,
        )
        sentiment = sentiment_breakdown_for_ticker(daily, ticker)
        model_summary = evaluation_summary(insight_evaluations, ticker)
        rows.append(
            {
                "ticker": ticker,
                "company": TICKER_COMPANY_NAMES.get(ticker, ticker),
                "headlines": int(sentiment["headline_count"]),
                "positive_pct": sentiment["positive_pct"],
                "neutral_pct": sentiment["neutral_pct"],
                "negative_pct": sentiment["negative_pct"],
                "quantity_owned": trade_summary["current_quantity"],
                "avg_buy_price_cad": trade_summary["avg_buy_price_cad"],
                "last_price_cad": trade_summary["last_price_cad"],
                "market_value_cad": trade_summary["market_value_cad"],
                "unrealized_pnl_cad": trade_summary["unrealized_pnl_cad"],
                "open_return_pct": trade_summary["open_return_pct"],
                "model_win_rate_pct": model_summary["real_win_rate_pct"]
                or model_summary["win_rate_pct"],
                "avg_market_move_pct": model_summary["avg_forward_return_pct"],
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["market_value_cad", "headlines"],
        ascending=[False, False],
    )


def ordered_dashboard_tickers(latest: pd.DataFrame, daily: pd.DataFrame) -> list[str]:
    data_tickers: list[str] = []
    for dataframe in (latest, daily):
        if dataframe.empty or "ticker" not in dataframe.columns:
            continue
        data_tickers.extend(
            dataframe["ticker"].dropna().astype(str).str.upper().tolist()
        )
    return list(dict.fromkeys(settings.default_tickers + data_tickers))


def build_sentiment_chart(dataframe: pd.DataFrame):
    if dataframe.empty:
        return None

    chart_data = dataframe.copy()
    chart_data["sentiment_date"] = pd.to_datetime(
        chart_data["sentiment_date"],
        errors="coerce",
    )
    chart_data = chart_data.sort_values("sentiment_date")
    value_column = (
        "rolling_7_day_volume_weighted_sentiment_index"
        if "rolling_7_day_volume_weighted_sentiment_index" in chart_data.columns
        else "avg_compound_score"
    )

    fig, ax = plt.subplots(figsize=(10, 4))
    for ticker, group in chart_data.groupby("ticker"):
        ax.plot(group["sentiment_date"], group[value_column], marker="o", label=ticker)
    ax.axhline(0, color="gray", linewidth=1)
    ax.set_title("Sentiment score over time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Score")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    return fig


def build_portfolio_chart(dataframe: pd.DataFrame):
    if dataframe.empty:
        return None

    chart_data = dataframe.copy()
    chart_data["snapshot_date"] = pd.to_datetime(
        chart_data["snapshot_date"],
        errors="coerce",
    )
    chart_data = chart_data.sort_values("snapshot_date")

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(chart_data["snapshot_date"], chart_data["total_equity_cad"], marker="o")
    ax.axhline(settings.portfolio_initial_cash_cad, color="gray", linewidth=1)
    ax.set_title("Six-month paper backtest")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio value (CAD)")
    fig.autofmt_xdate()
    return fig


def load_app_data() -> tuple[dict[str, pd.DataFrame], str]:
    if use_demo_data():
        return load_demo_dashboard_data(), "Demo data"
    if use_local_data():
        return load_local_dashboard_data(), "Local MySQL"
    return load_dashboard_data(), "Snowflake"


def dashboard_summary_metrics(
    daily: pd.DataFrame,
    signals: pd.DataFrame,
    portfolio: dict[str, float | str | None],
) -> tuple[int, int, int]:
    latest_headline_count = 0
    if not daily.empty and "sentiment_date" in daily.columns:
        latest_date = daily["sentiment_date"].max()
        latest_daily = daily[daily["sentiment_date"] == latest_date]
        latest_headline_count = int(numeric_column(latest_daily, "headline_count").sum())

    buy_count = 0
    sell_count = 0
    if not signals.empty and "recommendation" in signals.columns:
        latest_signals = signals.copy()
        if "sentiment_date" in latest_signals.columns:
            latest_date = latest_signals["sentiment_date"].max()
            latest_signals = latest_signals[latest_signals["sentiment_date"] == latest_date]
        recommendations = latest_signals["recommendation"].fillna("").astype(str)
        buy_count = int(recommendations.isin(["buy", "strong_buy"]).sum())
        sell_count = int(recommendations.isin(["sell", "trim"]).sum())

    return latest_headline_count, buy_count, sell_count


def write_summary_line(items: dict[str, str]) -> None:
    parts = [f"**{label}:** {value}" for label, value in items.items()]
    st.markdown(" | ".join(parts))


def format_currency(value: object, currency: str = "CAD") -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"${safe_float(value):,.2f} {currency}"


def format_currency_short(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"CAD {safe_float(value):,.0f}"


def format_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{safe_float(value):+.2f}%"


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def numeric_column(
    dataframe: pd.DataFrame,
    column: str,
    default: float = 0.0,
) -> pd.Series:
    if column not in dataframe.columns:
        return pd.Series(default, index=dataframe.index, dtype="float64")
    return pd.to_numeric(dataframe[column], errors="coerce").fillna(default)


def _signal_from_score(score: float) -> str:
    if score >= settings.positive_signal_threshold:
        return "positive"
    if score <= settings.negative_signal_threshold:
        return "negative"
    return "neutral"


def _latest_headline_lookup(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty or "ticker" not in latest.columns:
        return pd.DataFrame()

    headline_lookup = select_existing_columns(
        latest,
        ["ticker", "headline", "source", "published_at_utc"],
    ).copy()
    if headline_lookup.empty or "headline" not in headline_lookup.columns:
        return pd.DataFrame()

    headline_lookup["ticker"] = headline_lookup["ticker"].astype(str).str.upper()
    if "published_at_utc" in headline_lookup.columns:
        headline_lookup["published_at_utc"] = pd.to_datetime(
            headline_lookup["published_at_utc"],
            errors="coerce",
            utc=True,
        )
        headline_lookup = headline_lookup.sort_values("published_at_utc", ascending=False)

    headline_lookup = headline_lookup.groupby("ticker", as_index=False).head(1)
    return headline_lookup.rename(
        columns={
            "headline": "latest_headline",
            "source": "latest_source",
            "published_at_utc": "latest_headline_at_utc",
        }
    )


def choose_sidebar_tickers(
    all_tickers: list[str],
    default_tickers: list[str],
) -> list[str]:
    ticker_lookup = set(all_tickers)
    ticker_mode = st.selectbox(
        "Ticker view",
        [
            "Highest article volume",
            "S&P 500 range",
            "Type a short list",
        ],
    )

    if ticker_mode == "S&P 500 range":
        start_ticker = st.selectbox("Start at ticker", all_tickers)
        start_index = all_tickers.index(start_ticker)
        count = st.slider("Tickers to show", min_value=3, max_value=25, value=12)
        selected = all_tickers[start_index : start_index + count]
    elif ticker_mode == "Type a short list":
        ticker_text = st.text_input("Tickers", value=", ".join(default_tickers[:6]))
        requested = [
            ticker.strip().upper()
            for ticker in ticker_text.split(",")
            if ticker.strip()
        ]
        selected = [
            ticker
            for ticker in dict.fromkeys(requested)
            if ticker in ticker_lookup
        ]
    else:
        count = st.slider("Tickers to show", min_value=3, max_value=25, value=12)
        selected = default_tickers[:count]

    st.caption(
        f"Showing {len(selected)} tickers from the {len(settings.default_tickers)}-symbol S&P 500 list."
    )
    return selected


def main() -> None:
    apply_student_theme()
    st.title("Quicksilver")

    try:
        data, mode_label = load_app_data()
    except Exception as error:
        st.error("Could not load dashboard data.")
        st.exception(error)
        return

    latest = data.get("latest", pd.DataFrame())
    daily = data.get("daily", pd.DataFrame())
    rolling = data.get("rolling", pd.DataFrame())
    signals = data.get("signals", pd.DataFrame())
    price_quotes = data.get("price_quotes", pd.DataFrame())
    portfolio_positions = data.get("portfolio_positions", pd.DataFrame())
    portfolio_trades = data.get("portfolio_trades", pd.DataFrame())
    portfolio_snapshots = data.get("portfolio_snapshots", pd.DataFrame())
    pipeline_run_logs = data.get("pipeline_run_logs", pd.DataFrame())
    insight_evaluations = data.get("insight_evaluations", pd.DataFrame())
    health_alerts = data.get("health_alerts", pd.DataFrame())

    all_tickers = ordered_dashboard_tickers(latest, daily)
    default_tickers = default_selected_tickers(daily, all_tickers)

    with st.sidebar:
        st.header("Settings")
        st.write(mode_label)
        selected_tickers = choose_sidebar_tickers(all_tickers, default_tickers)
        refresh_prices = st.checkbox("Refresh latest prices", value=False)
        if st.button("Refresh data"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    latest = filter_by_ticker(latest, selected_tickers)
    daily = filter_by_ticker(daily, selected_tickers)
    rolling = filter_by_ticker(rolling, selected_tickers)
    signals = filter_by_ticker(signals, selected_tickers)

    live_quotes = pd.DataFrame()
    if refresh_prices and selected_tickers:
        live_quotes = fetch_live_price_quotes(
            tuple(selected_tickers[:10]),
            date.today().isoformat(),
        )
    watchlist_prices = build_watchlist_price_frame(
        selected_tickers,
        latest_price_quotes(price_quotes),
        live_quotes,
    )
    hot_stocks = build_hot_stocks(signals, latest)
    portfolio = portfolio_summary(data.get("portfolio_runs", pd.DataFrame()), portfolio_snapshots)
    stock_performance = build_portfolio_stock_frame(
        selected_tickers,
        portfolio_positions,
        portfolio_trades,
        daily,
        insight_evaluations,
        watchlist_prices,
    )

    st.caption(
        f"S&P 500 universe loaded: {len(settings.default_tickers)} symbols. "
        f"Current view: {len(selected_tickers)} tickers."
    )
    write_summary_line(
        {
            "Portfolio value": format_currency_short(portfolio["total_equity_cad"]),
            "Profit": format_currency_short(portfolio["profit_cad"]),
            "Open status notes": f"{len(filter_open_health_alerts(health_alerts)):,}",
        }
    )

    tab_recs, tab_backtest, tab_headlines, tab_pipeline = st.tabs(
        ["Recommendations", "Backtest", "Headlines", "Pipeline"]
    )

    with tab_recs:
        st.subheader("Latest recommendations")
        latest_headline_count, buy_count, sell_count = dashboard_summary_metrics(
            daily,
            signals,
            portfolio,
        )
        write_summary_line(
            {
                "Latest headlines": f"{latest_headline_count:,}",
                "Buy ideas": f"{buy_count:,}",
                "Sell or trim ideas": f"{sell_count:,}",
                "Paper return": format_percent(portfolio["return_pct"]),
            }
        )
        if hot_stocks.empty:
            st.info("Run the pipeline to create recommendations.")
        else:
            st.dataframe(hot_stocks, hide_index=True, width="stretch")

        st.subheader("Ticker prices")
        st.dataframe(watchlist_prices, hide_index=True, width="stretch")

    with tab_backtest:
        st.subheader("Paper backtest")
        chart = build_portfolio_chart(portfolio_snapshots)
        if chart is None:
            st.info("No backtest snapshots yet.")
        else:
            st.pyplot(chart, width="stretch")

        st.subheader("Ticker results")
        if stock_performance.empty:
            st.info("No ticker-level backtest rows yet.")
        else:
            st.dataframe(stock_performance, hide_index=True, width="stretch")

    with tab_headlines:
        st.subheader("Recent headlines")
        headline_columns = [
            "ticker",
            "sentiment_label",
            "compound_score",
            "confidence",
            "source",
            "published_at_utc",
            "headline",
        ]
        st.dataframe(
            select_existing_columns(latest, headline_columns),
            hide_index=True,
            width="stretch",
        )

        st.subheader("Sentiment trend")
        chart = build_sentiment_chart(rolling if not rolling.empty else daily)
        if chart is None:
            st.info("No sentiment trend yet.")
        else:
            st.pyplot(chart, width="stretch")

    with tab_pipeline:
        st.subheader("Pipeline runs")
        run_columns = [
            "started_at_utc",
            "finished_at_utc",
            "status",
            "ticker_count",
            "raw_headlines_collected",
            "scored_headlines",
            "insights_generated",
            "trades_executed",
            "error_message",
        ]
        runs = pipeline_run_logs
        if not runs.empty and "started_at_utc" in runs.columns:
            runs = runs.sort_values("started_at_utc", ascending=False)
        st.dataframe(
            select_existing_columns(runs, run_columns),
            hide_index=True,
            width="stretch",
        )

        st.subheader("Local status")
        status_columns = ["detected_at_utc", "severity", "alert_type", "message", "status"]
        st.dataframe(
            select_existing_columns(health_alerts, status_columns),
            hide_index=True,
            width="stretch",
        )


if __name__ == "__main__":
    main()
