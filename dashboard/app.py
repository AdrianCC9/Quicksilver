import sys
from datetime import date
from html import escape
from pathlib import Path
from textwrap import dedent

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
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


sns.set_theme(style="whitegrid")

DEFAULT_TICKER_SELECTION_LIMIT = 12
PAGE_OPTIONS = ["Dashboard", "Mock Market", "Trends", "Pipeline", "Data"]
PAGE_SLUGS = {
    "Dashboard": "dashboard",
    "Mock Market": "mock-market",
    "Trends": "trends",
    "Pipeline": "pipeline",
    "Data": "data",
}
PAGE_BY_SLUG = {slug: page for page, slug in PAGE_SLUGS.items()}

st.set_page_config(
    page_title="Quicksilver Dashboard",
    page_icon="QS",
    layout="wide",
    initial_sidebar_state="expanded",
)


def clean_html(html: str) -> str:
    return "\n".join(line.strip() for line in dedent(html).strip().splitlines())


def render_html(html: str) -> None:
    st.markdown(clean_html(html), unsafe_allow_html=True)


def filter_by_ticker(dataframe: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    if dataframe.empty or not tickers or "ticker" not in dataframe.columns:
        return dataframe

    return dataframe[dataframe["ticker"].isin(tickers)]


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
        .drop_duplicates()
        .tolist()
    )
    return list(dict.fromkeys(ranked_tickers + all_tickers))[:limit]


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
        palette="crest",
        ax=ax,
    )
    ax.axhline(0, color="#6b7280", linewidth=1, alpha=0.6)
    ax.set_title("Ticker Sentiment Trend")
    ax.set_xlabel("Date")
    ax.set_ylabel("7-day volume-weighted sentiment")
    ax.legend(title="Ticker", bbox_to_anchor=(1.01, 1), loc="upper left")
    ax.grid(color="#e5e7eb", linewidth=0.8)
    ax.set_facecolor("#ffffff")
    fig.patch.set_facecolor("#ffffff")
    fig.subplots_adjust(right=0.82)
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
        color="#15803d",
        ax=ax,
    )
    sns.lineplot(
        data=chart_data,
        x="sentiment_date",
        y="rolling_7_day_volume_weighted_sentiment_index",
        marker="o",
        label="7-day rolling index",
        color="#86efac",
        ax=ax,
    )
    ax.axhline(0, color="#6b7280", linewidth=1, alpha=0.6)
    ax.set_title("Market Sentiment Index")
    ax.set_xlabel("Date")
    ax.set_ylabel("Sentiment index")
    ax.grid(color="#e5e7eb", linewidth=0.8)
    ax.set_facecolor("#ffffff")
    fig.patch.set_facecolor("#ffffff")
    fig.subplots_adjust(right=0.96)
    return fig


def build_portfolio_chart(dataframe: pd.DataFrame):
    chart_data = dataframe.copy()

    if chart_data.empty:
        return None

    chart_data["snapshot_date"] = pd.to_datetime(chart_data["snapshot_date"])
    chart_data = chart_data.sort_values("snapshot_date")

    fig, ax = plt.subplots(figsize=(13, 4.2))
    sns.lineplot(
        data=chart_data,
        x="snapshot_date",
        y="total_equity_cad",
        marker="o",
        color="#15803d",
        linewidth=2.5,
        label="Total equity (CAD)",
        ax=ax,
    )
    ax.fill_between(
        chart_data["snapshot_date"],
        chart_data["total_equity_cad"],
        settings.portfolio_initial_cash_cad,
        color="#86efac",
        alpha=0.22,
    )
    ax.axhline(
        settings.portfolio_initial_cash_cad,
        color="#111827",
        linewidth=1,
        alpha=0.55,
        label="Starting cash",
    )
    ax.set_title("Mock Portfolio Equity Curve")
    ax.set_xlabel("Date")
    ax.set_ylabel("CAD")
    ax.grid(color="#e5e7eb", linewidth=0.8)
    ax.set_facecolor("#ffffff")
    fig.patch.set_facecolor("#ffffff")
    fig.subplots_adjust(right=0.96)
    return fig


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


def apply_dashboard_theme() -> None:
    render_html(
        """
        <style>
            :root {
                --qs-green: #15803d;
                --qs-green-dark: #166534;
                --qs-green-soft: #ecfdf3;
                --qs-green-mid: #86efac;
                --qs-ink: #111827;
                --qs-muted: #6b7280;
                --qs-border: #e5e7eb;
                --qs-panel: #ffffff;
                --qs-page: #ffffff;
                --qs-good: #16a34a;
                --qs-bad: #ef4444;
                --qs-warn: #d97706;
                --qs-neutral: #94a3b8;
            }
            .stApp {
                background: var(--qs-page);
                color: var(--qs-ink);
            }
            .block-container {
                max-width: 1500px;
                padding-top: 1.2rem;
                padding-bottom: 2.2rem;
                padding-left: 2rem;
                padding-right: 2rem;
            }
            section[data-testid="stSidebar"] {
                background: #ffffff;
                border-right: 1px solid var(--qs-border);
                box-shadow: 8px 0 22px rgba(17, 24, 39, 0.04);
            }
            section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
            section[data-testid="stSidebar"] label {
                color: var(--qs-muted);
            }
            section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
            button[kind="header"] {
                color: var(--qs-ink);
            }
            div[data-testid="stVerticalBlockBorderWrapper"] {
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                background: var(--qs-panel);
                box-shadow: 0 10px 24px rgba(17, 24, 39, 0.045);
            }
            .qs-brand {
                display: flex;
                align-items: center;
                gap: 0.65rem;
                margin: 0.35rem 0 1rem;
                color: var(--qs-ink);
            }
            .qs-logo {
                width: 32px;
                height: 32px;
                border-radius: 6px;
                background: linear-gradient(135deg, #166534 0%, #22c55e 100%);
                box-shadow: 0 8px 18px rgba(21, 128, 61, 0.18);
            }
            .qs-brand-name {
                font-weight: 800;
                letter-spacing: 0;
                font-size: 1.25rem;
            }
            .qs-sidebar-caption {
                color: var(--qs-muted);
                font-size: 0.78rem;
                margin: 0.5rem 0 1rem;
            }
            .qs-side-nav {
                display: grid;
                gap: 0.35rem;
                margin: 0.75rem 0 1.2rem;
            }
            .qs-side-link {
                display: flex;
                align-items: center;
                gap: 0.62rem;
                color: #4b5563 !important;
                font-weight: 650;
                padding: 0.66rem 0.76rem;
                border-radius: 6px;
                text-decoration: none !important;
                border: 1px solid transparent;
            }
            .qs-side-link:hover,
            .qs-side-link.active {
                color: var(--qs-green-dark) !important;
                background: var(--qs-green-soft);
                border-color: #bbf7d0;
            }
            .qs-side-dot {
                width: 0.68rem;
                height: 0.68rem;
                border-radius: 3px;
                background: #d1d5db;
                box-shadow: inset 0 0 0 2px rgba(255, 255, 255, 0.72);
            }
            .qs-side-link.active .qs-side-dot {
                background: var(--qs-green);
            }
            div[role="radiogroup"] label {
                border-radius: 6px;
                padding: 0.35rem 0.55rem;
            }
            div[role="radiogroup"] label:has(input:checked) {
                background: var(--qs-green-soft);
                color: var(--qs-green-dark);
                font-weight: 700;
            }
            div[data-testid="stSegmentedControl"] {
                margin: 0.25rem 0 1.1rem;
            }
            div[data-testid="stSegmentedControl"] button {
                border-radius: 6px;
                min-height: 2.45rem;
                font-weight: 700;
            }
            div[data-testid="stSegmentedControl"] button[aria-pressed="true"] {
                background: var(--qs-green-soft);
                border-color: #86efac;
                color: var(--qs-green-dark);
            }
            .qs-topbar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 1rem;
                margin-bottom: 1rem;
            }
            .qs-topbar h1 {
                margin: 0;
                color: var(--qs-ink);
                font-size: 1.85rem;
                line-height: 1.08;
                letter-spacing: 0;
            }
            .qs-topbar p {
                margin: 0.32rem 0 0;
                color: var(--qs-muted);
                font-size: 0.94rem;
            }
            .qs-search {
                min-width: 270px;
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                background: #ffffff;
                color: var(--qs-muted);
                padding: 0.8rem 1rem;
                box-shadow: 0 8px 18px rgba(17, 24, 39, 0.035);
            }
            .qs-status-row {
                display: grid;
                grid-template-columns: repeat(5, minmax(0, 1fr));
                gap: 0.75rem;
                margin: 0.8rem 0 1rem;
            }
            .qs-metric-card {
                background: #ffffff;
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                padding: 1rem;
                box-shadow: 0 8px 18px rgba(17, 24, 39, 0.035);
            }
            .qs-metric-label {
                color: var(--qs-muted);
                font-size: 0.76rem;
                margin-bottom: 0.35rem;
            }
            .qs-metric-value {
                color: var(--qs-ink);
                font-size: 1.45rem;
                line-height: 1.1;
                font-weight: 800;
            }
            .qs-metric-delta {
                color: var(--qs-good);
                font-size: 0.78rem;
                margin-top: 0.4rem;
            }
            .qs-stock-strip {
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 0.72rem;
                margin: 0.2rem 0 0.85rem;
            }
            .qs-stock-card {
                background: #ffffff;
                border: 1px solid var(--qs-border);
                border-left: 4px solid var(--qs-green);
                border-radius: 6px;
                padding: 0.85rem 0.9rem;
                min-height: 118px;
                color: var(--qs-ink);
                overflow: hidden;
                position: relative;
                box-shadow: 0 8px 18px rgba(17, 24, 39, 0.035);
            }
            .qs-stock-top {
                display: flex;
                justify-content: space-between;
                gap: 0.75rem;
                align-items: start;
                font-weight: 800;
                font-size: 0.92rem;
            }
            .qs-stock-company {
                color: var(--qs-muted);
                font-size: 0.76rem;
                margin-top: 0.16rem;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }
            .qs-stock-price {
                font-size: 1.15rem;
                font-weight: 900;
                margin-top: 1.05rem;
            }
            .qs-stock-source {
                color: var(--qs-muted);
                font-size: 0.7rem;
                margin-top: 0.2rem;
            }
            .qs-chip {
                display: inline-flex;
                align-items: center;
                border-radius: 4px;
                background: var(--qs-green-soft);
                color: var(--qs-green-dark);
                padding: 0.22rem 0.55rem;
                font-size: 0.72rem;
                font-weight: 800;
            }
            .qs-section-title {
                display: flex;
                align-items: baseline;
                justify-content: space-between;
                gap: 1rem;
                margin: 0.15rem 0 0.75rem;
            }
            .qs-section-title h2 {
                font-size: 1.16rem;
                line-height: 1.1;
                margin: 0;
                letter-spacing: 0;
            }
            .qs-section-title span {
                color: var(--qs-muted);
                font-size: 0.78rem;
            }
            .qs-hero-card {
                background: linear-gradient(135deg, #166534 0%, #15803d 54%, #22c55e 100%);
                color: #ffffff;
                border-radius: 6px;
                padding: 1.1rem 1.2rem;
                min-height: 150px;
                box-shadow: 0 14px 30px rgba(21, 128, 61, 0.18);
            }
            .qs-hero-card .qs-metric-label {
                color: rgba(255, 255, 255, 0.76);
            }
            .qs-hero-big {
                font-size: 2.15rem;
                line-height: 1.05;
                font-weight: 900;
                margin: 0.45rem 0;
            }
            .qs-hero-small {
                color: rgba(255, 255, 255, 0.78);
                font-size: 0.86rem;
            }
            .qs-sentiment-bar {
                display: flex;
                width: 100%;
                height: 14px;
                overflow: hidden;
                border-radius: 4px;
                background: #f3f4f6;
                margin: 0.65rem 0 0.8rem;
            }
            .qs-sentiment-segment-positive {
                background: #22c55e;
            }
            .qs-sentiment-segment-neutral {
                background: #94a3b8;
            }
            .qs-sentiment-segment-negative {
                background: #ef4444;
            }
            .qs-detail-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.72rem;
            }
            .qs-detail-tile {
                background: #ffffff;
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                padding: 0.85rem;
            }
            .qs-detail-tile span {
                display: block;
                color: var(--qs-muted);
                font-size: 0.72rem;
                margin-bottom: 0.25rem;
            }
            .qs-detail-tile strong {
                color: var(--qs-ink);
                font-size: 1rem;
            }
            .qs-note {
                color: var(--qs-muted);
                font-size: 0.82rem;
                margin: 0;
            }
            div[data-testid="stMetric"] {
                background: #ffffff;
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                padding: 0.8rem 0.9rem;
                box-shadow: 0 8px 18px rgba(17, 24, 39, 0.035);
            }
            div[data-testid="stMetricLabel"] p {
                color: var(--qs-muted);
                font-size: 0.82rem;
            }
            div[data-testid="stMetricValue"] {
                color: var(--qs-ink);
            }
            .stButton > button,
            .stDownloadButton > button {
                border-radius: 6px;
                border: 1px solid var(--qs-border);
                background: #ffffff;
                color: var(--qs-ink);
            }
            div[data-testid="stSelectbox"] div,
            div[data-testid="stTextInput"] input {
                border-radius: 6px !important;
            }
            div[data-testid="stDataFrame"] {
                background: #ffffff !important;
                border: 1px solid var(--qs-border);
                border-radius: 6px;
                overflow: hidden;
                box-shadow: 0 8px 18px rgba(17, 24, 39, 0.035);
                color-scheme: light;
            }
            div[data-testid="stDataFrame"] * {
                color-scheme: light;
            }
            div[data-testid="stDataFrame"] canvas,
            div[data-testid="stDataFrame"] [role="grid"],
            div[data-testid="stDataFrame"] [data-testid="stTable"] {
                background: #ffffff !important;
            }
            div[data-testid="stDataFrameResizable"] {
                background: #ffffff !important;
            }
            table {
                background: #ffffff !important;
                color: var(--qs-ink) !important;
            }
            thead tr,
            tbody tr {
                background: #ffffff !important;
            }
            th,
            td {
                background: #ffffff !important;
                color: var(--qs-ink) !important;
                border-color: var(--qs-border) !important;
            }
            @media (max-width: 1100px) {
                .qs-status-row,
                .qs-stock-strip,
                .qs-detail-grid {
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }
                .qs-search {
                    display: none;
                }
            }
            @media (max-width: 720px) {
                .block-container {
                    padding-left: 1rem;
                    padding-right: 1rem;
                }
                .qs-status-row,
                .qs-stock-strip,
                .qs-detail-grid {
                    grid-template-columns: 1fr;
                }
            }
        </style>
        """
    )


def display_dashboard_header(mode_label: str) -> None:
    render_html(
        f"""
        <section class="qs-topbar">
            <div>
                <h1>Quicksilver</h1>
                <p>{escape(mode_label)} | stock sentiment, mock-market returns, and pipeline health</p>
            </div>
            <div class="qs-search">Search stocks, headlines, signals</div>
        </section>
        """
    )


def dashboard_page_slug(page: str) -> str:
    return PAGE_SLUGS.get(page, "dashboard")


def dashboard_page_from_query() -> str:
    raw_page = st.query_params.get("page", "")
    if isinstance(raw_page, list):
        raw_page = raw_page[0] if raw_page else ""

    normalized = str(raw_page).strip().lower().replace("_", "-").replace(" ", "-")
    return PAGE_BY_SLUG.get(normalized, PAGE_OPTIONS[0])


def render_sidebar_nav(active_page: str) -> None:
    links = []
    for page in PAGE_OPTIONS:
        active_class = " active" if page == active_page else ""
        links.append(
            (
                f'<a class="qs-side-link{active_class}" '
                f'href="?page={dashboard_page_slug(page)}" target="_self">'
                '<span class="qs-side-dot"></span>'
                f"<span>{escape(page)}</span>"
                "</a>"
            )
        )

    render_html(f"""<div class="qs-side-nav">{''.join(links)}</div>""")


def render_page_switcher(initial_page: str) -> str:
    requested_slug = dashboard_page_slug(initial_page)
    if st.session_state.get("_dashboard_page_slug") != requested_slug:
        st.session_state["dashboard_page_nav"] = initial_page
        st.session_state["_dashboard_page_slug"] = requested_slug

    page = st.segmented_control(
        "Dashboard view",
        PAGE_OPTIONS,
        default=initial_page,
        key="dashboard_page_nav",
        label_visibility="collapsed",
        width="stretch",
    )
    selected_page = str(page or initial_page)
    selected_slug = dashboard_page_slug(selected_page)
    if st.session_state.get("_dashboard_page_slug") != selected_slug:
        st.session_state["_dashboard_page_slug"] = selected_slug
        st.query_params["page"] = selected_slug

    return selected_page


def dashboard_mode_label(demo_mode: bool, local_mode: bool) -> str:
    if demo_mode:
        return "Demo mode (generated sample data)"
    if local_mode:
        return "Local MySQL mode (live RSS data)"
    return "Snowflake mode (optional cloud path)"


def ordered_dashboard_tickers(
    latest: pd.DataFrame,
    daily: pd.DataFrame,
) -> list[str]:
    data_tickers: list[str] = []
    for dataframe in (latest, daily):
        if dataframe.empty or "ticker" not in dataframe.columns:
            continue
        data_tickers.extend(
            dataframe["ticker"].dropna().astype(str).str.upper().tolist()
        )

    return list(dict.fromkeys(settings.default_tickers + data_tickers))


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
    quotes["quote_timestamp"] = pd.to_datetime(
        quotes["quote_date"],
        errors="coerce",
    )
    quotes["quote_date"] = quotes["quote_timestamp"].dt.date
    quotes["close_price_usd"] = pd.to_numeric(
        quotes.get("close_price_usd"),
        errors="coerce",
    )
    quotes = quotes.dropna(subset=["ticker"])
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
    combined["quote_timestamp"] = pd.to_datetime(
        combined["quote_date"],
        errors="coerce",
    )
    combined["quote_date"] = combined["quote_timestamp"].dt.date
    combined["close_price_usd"] = pd.to_numeric(
        combined["close_price_usd"],
        errors="coerce",
    )
    data_source = combined["data_source"].fillna("").astype(str).str.lower()
    combined["is_missing_price"] = combined["close_price_usd"].isna()
    combined["is_synthetic"] = data_source.str.contains("synthetic|demo|unavailable")
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
    if not quotes.empty:
        base = base.merge(quotes, on="ticker", how="left")
    else:
        base["quote_date"] = pd.NaT
        base["close_price_usd"] = pd.NA
        base["data_source"] = pd.NA

    return base[
        [
            "ticker",
            "company",
            "sector",
            "close_price_usd",
            "quote_date",
            "data_source",
        ]
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
        working["avg_compound_score"] = numeric_column(
            working,
            "compound_score",
            default=0.0,
        )
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
    working["hot_signal_score"] = numeric_column(
        working,
        score_column,
        default=0.0,
    )
    working["headline_count"] = numeric_column(
        working,
        "headline_count",
        default=0.0,
    )
    working["political_headline_count"] = numeric_column(
        working,
        "political_headline_count",
        default=0.0,
    )
    working["compound_score_zscore"] = numeric_column(
        working,
        "compound_score_zscore",
        default=0.0,
    )
    working["opportunity_score"] = numeric_column(
        working,
        "opportunity_score",
        default=0.0,
    )

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


def render_stock_cards(
    watchlist_prices: pd.DataFrame,
    hot_stocks: pd.DataFrame,
    max_cards: int = 8,
) -> None:
    if watchlist_prices.empty:
        st.info("No watchlist prices are available yet.")
        return

    signal_lookup = {}
    if not hot_stocks.empty and "ticker" in hot_stocks.columns:
        for _, row in hot_stocks.iterrows():
            signal_lookup[str(row["ticker"]).upper()] = row

    cards = []
    accents = ["#15803d", "#16a34a", "#22c55e", "#65a30d", "#0f766e", "#059669"]

    for index, row in watchlist_prices.head(max_cards).iterrows():
        ticker = str(row.get("ticker", "")).upper()
        signal_row = signal_lookup.get(ticker)
        signal_label = (
            str(signal_row.get("signal_label", "watch")).title()
            if signal_row is not None
            else "Watch"
        )
        heat = (
            format_number(signal_row.get("news_heat_score"), decimals=1)
            if signal_row is not None and "news_heat_score" in signal_row
            else "n/a"
        )
        cards.append(
            clean_html(
                f"""
                <div class="qs-stock-card" style="border-left-color: {accents[index % len(accents)]};">
                    <div class="qs-stock-top">
                        <div>
                            <div>{escape(ticker)}</div>
                            <div class="qs-stock-company">{escape(str(row.get("company", ticker)))}</div>
                        </div>
                        <span class="qs-chip">{escape(signal_label)}</span>
                    </div>
                    <div class="qs-stock-price">{escape(format_currency(row.get("close_price_usd"), "USD"))}</div>
                    <div class="qs-stock-source">Quote: {escape(str(row.get("data_source", "n/a")))} | Heat {escape(heat)}</div>
                </div>
                """
            )
        )

    render_html(f"""<div class="qs-stock-strip">{''.join(cards)}</div>""")


def render_metric_grid(metrics: list[dict[str, str]]) -> None:
    cards = []
    for metric in metrics:
        delta = metric.get("delta", "")
        delta_html = (
            clean_html(f"""<div class="qs-metric-delta">{escape(delta)}</div>""")
            if delta
            else ""
        )
        cards.append(
            clean_html(
                f"""
                <div class="qs-metric-card">
                    <div class="qs-metric-label">{escape(metric["label"])}</div>
                    <div class="qs-metric-value">{escape(metric["value"])}</div>
                    {delta_html}
                </div>
                """
            )
        )

    render_html(f"""<div class="qs-status-row">{''.join(cards)}</div>""")


def render_section_title(title: str, detail: str = "") -> None:
    detail_html = f"<span>{escape(detail)}</span>" if detail else ""
    render_html(
        f"""
        <div class="qs-section-title">
            <h2>{escape(title)}</h2>
            {detail_html}
        </div>
        """
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
        if "cumulative_return_pct" in latest_snapshot
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
    if evaluations.empty:
        return {
            "evaluated": 0,
            "real_evaluated": 0,
            "win_rate_pct": 0.0,
            "real_win_rate_pct": 0.0,
            "avg_forward_return_pct": 0.0,
        }

    working = evaluations.copy()
    if ticker and "ticker" in working.columns:
        working = working[working["ticker"].astype(str).str.upper() == ticker.upper()]
    if working.empty:
        return {
            "evaluated": 0,
            "real_evaluated": 0,
            "win_rate_pct": 0.0,
            "real_win_rate_pct": 0.0,
            "avg_forward_return_pct": 0.0,
        }

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
        "avg_forward_return_pct": float(
            numeric_column(working, "forward_return_pct").mean()
        ),
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


def render_sentiment_breakdown(breakdown: dict[str, float]) -> None:
    positive_pct = max(breakdown["positive_pct"], 0.0)
    neutral_pct = max(breakdown["neutral_pct"], 0.0)
    negative_pct = max(breakdown["negative_pct"], 0.0)
    render_html(
        f"""
        <div class="qs-sentiment-bar">
            <div class="qs-sentiment-segment-positive" style="width: {positive_pct:.2f}%"></div>
            <div class="qs-sentiment-segment-neutral" style="width: {neutral_pct:.2f}%"></div>
            <div class="qs-sentiment-segment-negative" style="width: {negative_pct:.2f}%"></div>
        </div>
        <div class="qs-detail-grid">
            <div class="qs-detail-tile"><span>Positive</span><strong>{positive_pct:.1f}% ({int(breakdown["positive_count"]):,})</strong></div>
            <div class="qs-detail-tile"><span>Neutral</span><strong>{neutral_pct:.1f}% ({int(breakdown["neutral_count"]):,})</strong></div>
            <div class="qs-detail-tile"><span>Negative</span><strong>{negative_pct:.1f}% ({int(breakdown["negative_count"]):,})</strong></div>
        </div>
        """
    )


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
            numeric_column(buy_trades, "price_cad") * numeric_column(buy_trades, "quantity")
        ).sum() / bought_quantity
    else:
        avg_buy_price = 0.0

    current_quantity = numeric_column(positions, "quantity").sum() if not positions.empty else 0.0
    market_value = (
        numeric_column(positions, "market_value_cad").sum()
        if not positions.empty
        else 0.0
    )
    unrealized_pnl = (
        numeric_column(positions, "unrealized_pnl_cad").sum()
        if not positions.empty
        else 0.0
    )
    last_price = 0.0
    if not positions.empty and current_quantity:
        last_price = (
            numeric_column(positions, "last_price_cad") * numeric_column(positions, "quantity")
        ).sum() / current_quantity
    elif not watchlist_prices.empty and "ticker" in watchlist_prices.columns:
        quote_rows = watchlist_prices[
            watchlist_prices["ticker"].astype(str).str.upper() == ticker
        ]
        if not quote_rows.empty:
            last_price = safe_float(quote_rows.iloc[0].get("close_price_usd")) * settings.usd_to_cad_rate

    open_cost = current_quantity * avg_buy_price
    open_return_pct = (unrealized_pnl / open_cost) * 100 if open_cost else 0.0
    latest_trade_at = ""
    if not trades.empty and "traded_at_utc" in trades.columns:
        latest_trade_at = str(
            pd.to_datetime(trades["traded_at_utc"], errors="coerce").max()
        )

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


def render_stock_trade_detail(
    ticker: str,
    portfolio_positions: pd.DataFrame,
    portfolio_trades: pd.DataFrame,
    daily: pd.DataFrame,
    insight_evaluations: pd.DataFrame,
    watchlist_prices: pd.DataFrame,
) -> None:
    trade_summary = trade_summary_for_ticker(
        ticker,
        portfolio_positions,
        portfolio_trades,
        watchlist_prices,
    )
    sentiment = sentiment_breakdown_for_ticker(daily, ticker)
    model_summary = evaluation_summary(insight_evaluations, ticker)

    render_section_title(
        f"{ticker} drill-down",
        TICKER_COMPANY_NAMES.get(ticker, ticker),
    )
    render_sentiment_breakdown(sentiment)

    render_html(
        f"""
        <div class="qs-detail-grid" style="margin-top: 0.8rem;">
            <div class="qs-detail-tile"><span>Bought</span><strong>{trade_summary["bought_quantity"]:.4g} shares @ {escape(format_currency(trade_summary["avg_buy_price_cad"], "CAD"))}</strong></div>
            <div class="qs-detail-tile"><span>Currently held</span><strong>{trade_summary["current_quantity"]:.4g} shares</strong></div>
            <div class="qs-detail-tile"><span>Current price</span><strong>{escape(format_currency(trade_summary["last_price_cad"], "CAD"))}</strong></div>
            <div class="qs-detail-tile"><span>Market value</span><strong>{escape(format_currency(trade_summary["market_value_cad"], "CAD"))}</strong></div>
            <div class="qs-detail-tile"><span>Open P&L</span><strong>{escape(format_currency(trade_summary["unrealized_pnl_cad"], "CAD"))}</strong></div>
            <div class="qs-detail-tile"><span>Open return</span><strong>{escape(format_percent(trade_summary["open_return_pct"]))}</strong></div>
            <div class="qs-detail-tile"><span>Model win rate</span><strong>{model_summary["real_win_rate_pct"] or model_summary["win_rate_pct"]:.1f}%</strong></div>
            <div class="qs-detail-tile"><span>Avg market move</span><strong>{escape(format_percent(model_summary["avg_forward_return_pct"]))}</strong></div>
            <div class="qs-detail-tile"><span>Real evaluations</span><strong>{int(model_summary["real_evaluated"]):,}</strong></div>
        </div>
        """
    )

    if not portfolio_trades.empty and "ticker" in portfolio_trades.columns:
        trade_rows = portfolio_trades[
            portfolio_trades["ticker"].astype(str).str.upper() == ticker.upper()
        ].copy()
        if not trade_rows.empty:
            render_section_title("Trade ledger")
            trade_columns = [
                "traded_at_utc",
                "side",
                "quantity",
                "price_cad",
                "gross_cad",
                "reason",
            ]
            st.dataframe(
                select_existing_columns(
                    trade_rows.sort_values("traded_at_utc", ascending=False)
                    if "traded_at_utc" in trade_rows.columns
                    else trade_rows,
                    trade_columns,
                ),
                width="stretch",
                hide_index=True,
                column_config={
                    "quantity": st.column_config.NumberColumn("Quantity", format="%.4f"),
                    "price_cad": st.column_config.NumberColumn("Price (CAD)", format="$%.2f"),
                    "gross_cad": st.column_config.NumberColumn("Gross (CAD)", format="$%.2f"),
                },
            )


def latest_pipeline_metrics(
    pipeline_run_logs: pd.DataFrame,
    health_alerts: pd.DataFrame,
) -> list[dict[str, str]]:
    latest_run = pd.Series(dtype=object)
    if not pipeline_run_logs.empty and "started_at_utc" in pipeline_run_logs.columns:
        latest_run = pipeline_run_logs.sort_values(
            "started_at_utc",
            ascending=False,
        ).iloc[0]

    open_alerts = filter_open_health_alerts(health_alerts)
    return [
        {
            "label": "Last run",
            "value": str(latest_run.get("status", "No runs")),
            "delta": str(latest_run.get("started_at_utc", "")),
        },
        {
            "label": "Tickers scanned",
            "value": _format_count(latest_run.get("ticker_count", 0)),
        },
        {
            "label": "Headlines collected",
            "value": _format_count(latest_run.get("raw_headlines_collected", 0)),
        },
        {
            "label": "Insights generated",
            "value": _format_count(latest_run.get("insights_generated", 0)),
        },
        {
            "label": "Open alerts",
            "value": _format_count(len(open_alerts)),
        },
    ]


def dashboard_summary_metrics(
    daily: pd.DataFrame,
    signals: pd.DataFrame,
    market: pd.DataFrame,
    portfolio: dict[str, float | str | None],
) -> list[dict[str, str]]:
    if daily.empty:
        return [
            {"label": "Starting budget", "value": format_currency(portfolio["starting_cash_cad"], "CAD")},
            {"label": "Portfolio value", "value": format_currency(portfolio["total_equity_cad"], "CAD")},
            {"label": "Profit", "value": format_currency(portfolio["profit_cad"], "CAD")},
            {"label": "Return", "value": format_percent(portfolio["return_pct"])},
            {"label": "Headlines scored", "value": "0"},
        ]

    latest_date = daily["sentiment_date"].max()
    latest_daily = daily[daily["sentiment_date"] == latest_date]
    latest_signals = signals
    if not signals.empty and "sentiment_date" in signals.columns:
        latest_signal_date = signals["sentiment_date"].max()
        latest_signals = signals[signals["sentiment_date"] == latest_signal_date]

    active_signals = 0
    if not latest_signals.empty and {
        "is_positive_sentiment_signal",
        "is_negative_sentiment_signal",
    }.issubset(latest_signals.columns):
        active_signals = int(
            latest_signals["is_positive_sentiment_signal"].sum()
            + latest_signals["is_negative_sentiment_signal"].sum()
        )

    market_index = "n/a"
    if not market.empty and "volume_weighted_sentiment_index" in market.columns:
        latest_market_date = market["sentiment_date"].max()
        latest_market = market[market["sentiment_date"] == latest_market_date]
        if not latest_market.empty:
            market_index = format_number(latest_market["volume_weighted_sentiment_index"].iloc[0], 3)

    return [
        {
            "label": "Portfolio value",
            "value": format_currency(portfolio["total_equity_cad"], "CAD"),
            "delta": f"Budget {format_currency(portfolio['starting_cash_cad'], 'CAD')}",
        },
        {
            "label": "Profit",
            "value": format_currency(portfolio["profit_cad"], "CAD"),
            "delta": format_percent(portfolio["return_pct"]),
        },
        {
            "label": "Headlines scored",
            "value": f"{int(latest_daily['headline_count'].sum()):,}",
            "delta": f"Latest date {latest_date}",
        },
        {
            "label": "Active signals",
            "value": f"{active_signals:,}",
            "delta": "positive and negative",
        },
        {
            "label": "Market index",
            "value": market_index,
            "delta": "volume weighted",
        },
    ]


def format_currency(value: object, currency: str = "CAD") -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"${safe_float(value):,.2f} {currency}"


def format_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{safe_float(value):+.2f}%"


def format_number(value: object, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{safe_float(value):,.{decimals}f}"


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _signal_from_score(score: float) -> str:
    if score >= settings.positive_signal_threshold:
        return "positive"
    if score <= settings.negative_signal_threshold:
        return "negative"
    return "neutral"


def numeric_column(
    dataframe: pd.DataFrame,
    column: str,
    default: float = 0.0,
) -> pd.Series:
    if column not in dataframe.columns:
        return pd.Series(default, index=dataframe.index, dtype="float64")

    return pd.to_numeric(dataframe[column], errors="coerce").fillna(default)


def _latest_headline_lookup(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty or "ticker" not in latest.columns:
        return pd.DataFrame()

    headline_columns = [
        "ticker",
        "headline",
        "source",
        "published_at_utc",
    ]
    headline_lookup = select_existing_columns(latest, headline_columns).copy()
    if headline_lookup.empty or "headline" not in headline_lookup.columns:
        return pd.DataFrame()

    if "published_at_utc" in headline_lookup.columns:
        headline_lookup["published_at_utc"] = pd.to_datetime(
            headline_lookup["published_at_utc"],
            errors="coerce",
            utc=True,
        )
        headline_lookup = headline_lookup.sort_values(
            "published_at_utc",
            ascending=False,
        )

    headline_lookup = headline_lookup.groupby("ticker", as_index=False).head(1)
    headline_lookup = headline_lookup.rename(
        columns={
            "headline": "latest_headline",
            "source": "latest_source",
            "published_at_utc": "latest_headline_at_utc",
        }
    )
    return headline_lookup


def display_pipeline_overview(
    pipeline_run_logs: pd.DataFrame,
    audit: pd.DataFrame,
    health_alerts: pd.DataFrame,
    mode_label: str,
) -> None:
    latest_run = pd.Series(dtype=object)
    if not pipeline_run_logs.empty and "started_at_utc" in pipeline_run_logs.columns:
        latest_run = pipeline_run_logs.sort_values(
            "started_at_utc",
            ascending=False,
        ).iloc[0]

    open_alerts = filter_open_health_alerts(health_alerts)
    run_status = str(latest_run.get("status", "no runs"))
    raw_count = latest_run.get("raw_headlines_collected", 0)
    scored_count = latest_run.get("scored_headlines", 0)
    insights_count = latest_run.get("insights_generated", 0)
    tracked_count = None
    if not audit.empty and "tracked_ticker_count" in audit.columns:
        tracked_count = audit["tracked_ticker_count"].iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Storage", mode_label)
    col2.metric("Last run", run_status)
    col3.metric("Raw headlines", _format_count(raw_count))
    col4.metric("Insights", _format_count(insights_count))
    col5.metric("Open alerts", _format_count(len(open_alerts)))

    detail_parts = []
    if not latest_run.empty:
        started = latest_run.get("started_at_utc")
        detail_parts.append(f"latest run started {started}")
        detail_parts.append(f"scored {_format_count(scored_count)} headlines")
    if tracked_count is not None and not pd.isna(tracked_count):
        detail_parts.append(f"tracking {_format_count(tracked_count)} tickers")
    if detail_parts:
        st.caption(" | ".join(detail_parts))


def _format_count(value: object) -> str:
    try:
        if pd.isna(value):
            return "0"
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


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
    apply_dashboard_theme()

    demo_mode = use_demo_data()
    local_mode = use_local_data()
    mode_label = dashboard_mode_label(demo_mode, local_mode)
    requested_page = dashboard_page_from_query()

    with st.sidebar:
        render_html(
            """
            <div class="qs-brand">
                <div class="qs-logo"></div>
                <div class="qs-brand-name">Quicksilver</div>
            </div>
            <div class="qs-sidebar-caption">Stock intelligence workspace</div>
            """
        )
        st.caption("User Panel")
        render_sidebar_nav(requested_page)
        st.caption(mode_label)
        if st.button("Refresh data"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

    if demo_mode:
        st.sidebar.info("Generated sample data is active; no live portfolio is implied.")
        data = load_demo_dashboard_data()
    elif local_mode:
        try:
            data = load_local_dashboard_data()
        except Exception as error:
            st.error("Could not load dashboard data from local MySQL.")
            st.exception(error)
            return
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
    price_quotes = data.get("price_quotes", pd.DataFrame())
    portfolio_positions = data.get("portfolio_positions", pd.DataFrame())
    portfolio_trades = data.get("portfolio_trades", pd.DataFrame())
    portfolio_snapshots = data.get("portfolio_snapshots", pd.DataFrame())
    pipeline_run_logs = data.get("pipeline_run_logs", pd.DataFrame())
    insight_evaluations = data.get("insight_evaluations", pd.DataFrame())
    performance_summary = data.get("performance_summary", pd.DataFrame())
    health_alerts = data.get("health_alerts", pd.DataFrame())
    report_runs = data.get("report_runs", pd.DataFrame())

    all_tickers = ordered_dashboard_tickers(latest, daily)

    with st.sidebar:
        default_tickers = default_selected_tickers(daily, all_tickers)
        watchlist_mode = st.selectbox(
            "Watchlist",
            [
                "Interview watchlist",
                "Latest high-volume names",
                "All tracked tickers",
                "Custom comma list",
            ],
            index=0,
        )
        if watchlist_mode == "All tracked tickers":
            selected_tickers = all_tickers
        elif watchlist_mode == "Latest high-volume names":
            selected_tickers = default_tickers
        elif watchlist_mode == "Custom comma list":
            ticker_text = st.text_input(
                "Tickers",
                value=", ".join(default_tickers),
                help="Enter comma-separated ticker symbols.",
            )
            requested_tickers = [
                ticker.strip().upper()
                for ticker in ticker_text.split(",")
                if ticker.strip()
            ]
            selected_tickers = [
                ticker for ticker in dict.fromkeys(requested_tickers) if ticker in all_tickers
            ]
        else:
            preferred = [ticker for ticker in settings.default_tickers if ticker in all_tickers]
            selected_tickers = list(dict.fromkeys(preferred + default_tickers))[
                :DEFAULT_TICKER_SELECTION_LIMIT
            ]
        st.caption(
            f"{len(selected_tickers)} selected: {', '.join(selected_tickers[:8])}"
            + (" ..." if len(selected_tickers) > 8 else "")
        )
        live_prices_enabled = st.checkbox(
            "Refresh live prices",
            value=not demo_mode,
        )
        st.caption("Use the sidebar chevron to collapse this panel during the demo.")

    latest = filter_by_ticker(latest, selected_tickers)
    daily = filter_by_ticker(daily, selected_tickers)
    rolling = filter_by_ticker(rolling, selected_tickers)
    signals = filter_by_ticker(signals, selected_tickers)

    cached_quotes = latest_price_quotes(price_quotes)
    price_lookup_tickers = tuple(selected_tickers[:20])
    live_quotes = pd.DataFrame()
    if live_prices_enabled and price_lookup_tickers:
        with st.spinner("Refreshing price quotes..."):
            live_quotes = fetch_live_price_quotes(
                price_lookup_tickers,
                date.today().isoformat(),
            )
        if len(selected_tickers) > len(price_lookup_tickers):
            st.sidebar.caption("Live price lookup is capped at 20 selected tickers.")

    watchlist_prices = build_watchlist_price_frame(
        selected_tickers,
        cached_quotes,
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

    display_dashboard_header(mode_label)
    page = render_page_switcher(requested_page)

    if page == "Dashboard":
        render_metric_grid(dashboard_summary_metrics(daily, signals, market, portfolio))
        render_section_title("My Stocks", "latest quotes and hottest signals")
        render_stock_cards(watchlist_prices, hot_stocks)

        left_col, right_col = st.columns([1.08, 0.92])
        with left_col:
            with st.container(border=True):
                render_section_title("Portfolio Snapshot", "mock CAD portfolio")
                render_html(
                    f"""
                    <div class="qs-hero-card">
                        <div class="qs-metric-label">Total equity from $5,000 CAD budget</div>
                        <div class="qs-hero-big">{escape(format_currency(portfolio["total_equity_cad"], "CAD"))}</div>
                        <div class="qs-hero-small">Profit {escape(format_currency(portfolio["profit_cad"], "CAD"))} | Return {escape(format_percent(portfolio["return_pct"]))}</div>
                    </div>
                    """
                )
                snapshot_cols = st.columns(3)
                snapshot_cols[0].metric("Cash", format_currency(portfolio["cash_cad"], "CAD"))
                snapshot_cols[1].metric(
                    "Invested value",
                    format_currency(portfolio["positions_value_cad"], "CAD"),
                )
                snapshot_cols[2].metric("Quote source", str(portfolio["data_source"]))
                portfolio_chart = build_portfolio_chart(portfolio_snapshots)
                if portfolio_chart is not None:
                    st.pyplot(portfolio_chart, width="stretch")

        with right_col:
            with st.container(border=True):
                render_section_title("Hot Stocks From News")
                if hot_stocks.empty:
                    st.info("No scored headlines available yet.")
                else:
                    st.dataframe(
                        hot_stocks,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "ticker": st.column_config.TextColumn("Ticker", width="small"),
                            "news_heat_score": st.column_config.NumberColumn("Heat", format="%.2f"),
                            "signal_label": st.column_config.TextColumn("Signal"),
                            "headline_count": st.column_config.NumberColumn("Articles", format="%.0f"),
                            "political_headline_count": st.column_config.NumberColumn("Policy", format="%.0f"),
                            "hot_signal_score": st.column_config.NumberColumn("Score", format="%.3f"),
                            "why_hot": st.column_config.TextColumn("Why hot", width="large"),
                        },
                    )

        with st.container(border=True):
            render_section_title("Followed Stocks & Prices")
            if watchlist_prices.empty:
                st.info("No tickers selected.")
            else:
                st.dataframe(
                    watchlist_prices,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "ticker": st.column_config.TextColumn("Ticker", width="small"),
                        "company": st.column_config.TextColumn("Company"),
                        "sector": st.column_config.TextColumn("Sector"),
                        "close_price_usd": st.column_config.NumberColumn(
                            "Price (USD)",
                            format="$%.2f",
                        ),
                        "quote_date": st.column_config.DateColumn("Quote date"),
                        "data_source": st.column_config.TextColumn("Source"),
                    },
                )

    elif page == "Mock Market":
        render_metric_grid(
            [
                {"label": "Starting budget", "value": format_currency(portfolio["starting_cash_cad"], "CAD")},
                {"label": "Current equity", "value": format_currency(portfolio["total_equity_cad"], "CAD")},
                {
                    "label": "Profit",
                    "value": format_currency(portfolio["profit_cad"], "CAD"),
                    "delta": format_percent(portfolio["return_pct"]),
                },
                {"label": "Cash", "value": format_currency(portfolio["cash_cad"], "CAD")},
                {"label": "Invested", "value": format_currency(portfolio["positions_value_cad"], "CAD")},
            ]
        )

        with st.container(border=True):
            render_section_title("Mock Stock Market", "paper trading result against real quotes")
            if portfolio_snapshots.empty:
                st.info("No portfolio snapshots yet. Run the local pipeline to start the simulation.")
            else:
                portfolio_chart = build_portfolio_chart(portfolio_snapshots)
                if portfolio_chart is not None:
                    st.pyplot(portfolio_chart, width="stretch")

        stock_options = (
            stock_performance["ticker"].tolist()
            if not stock_performance.empty and "ticker" in stock_performance.columns
            else selected_tickers
        )
        stock_options = list(dict.fromkeys(stock_options))[:24]
        with st.container(border=True):
            render_section_title("Stock Drill-Down", "click a ticker to inspect model and trade behavior")
            if stock_options:
                selected_stock = st.segmented_control(
                    "Inspect stock",
                    stock_options,
                    default=stock_options[0],
                    label_visibility="collapsed",
                    width="stretch",
                )
                render_stock_trade_detail(
                    str(selected_stock or stock_options[0]),
                    portfolio_positions,
                    portfolio_trades,
                    daily,
                    insight_evaluations,
                    watchlist_prices,
                )
            else:
                st.info("No stock-level data is available yet.")

        with st.container(border=True):
            render_section_title("Stock-Level Performance Ledger")
            if stock_performance.empty:
                st.info("No stock-level performance rows are available yet.")
            else:
                st.dataframe(
                    stock_performance,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "ticker": st.column_config.TextColumn("Ticker", width="small"),
                        "positive_pct": st.column_config.NumberColumn("Positive", format="%.1f%%"),
                        "neutral_pct": st.column_config.NumberColumn("Neutral", format="%.1f%%"),
                        "negative_pct": st.column_config.NumberColumn("Negative", format="%.1f%%"),
                        "quantity_owned": st.column_config.NumberColumn("Shares held", format="%.4f"),
                        "avg_buy_price_cad": st.column_config.NumberColumn("Avg buy", format="$%.2f"),
                        "last_price_cad": st.column_config.NumberColumn("Last price", format="$%.2f"),
                        "market_value_cad": st.column_config.NumberColumn("Market value", format="$%.2f"),
                        "unrealized_pnl_cad": st.column_config.NumberColumn("Open P&L", format="$%.2f"),
                        "open_return_pct": st.column_config.NumberColumn("Open return", format="%.2f%%"),
                        "model_win_rate_pct": st.column_config.NumberColumn("Model win rate", format="%.1f%%"),
                        "avg_market_move_pct": st.column_config.NumberColumn("Avg market move", format="%.2f%%"),
                    },
                )

        with st.container(border=True):
            render_section_title("Open Positions & Recent Trades")
            left_portfolio, right_portfolio = st.columns(2)
            with left_portfolio:
                st.dataframe(portfolio_positions, width="stretch", hide_index=True)
            with right_portfolio:
                st.dataframe(
                    portfolio_trades.sort_values("traded_at_utc", ascending=False)
                    if not portfolio_trades.empty and "traded_at_utc" in portfolio_trades.columns
                    else portfolio_trades,
                    width="stretch",
                    hide_index=True,
                )

        with st.container(border=True):
            render_section_title("Model vs Real Market")
            if performance_summary.empty:
                st.info("No insight evaluations yet.")
            else:
                overall = performance_summary[
                    performance_summary["segment"] == "overall"
                ].iloc[0]
                perf1, perf2, perf3, perf4 = st.columns(4)
                perf1.metric("Evaluated insights", f"{int(overall['evaluated_insights']):,}")
                perf2.metric("Real quote evals", f"{int(overall['real_market_evaluations']):,}")
                real_evaluations = int(overall["real_market_evaluations"])
                win_rate = overall["real_win_rate_pct"] if real_evaluations else overall["win_rate_pct"]
                avg_return = (
                    overall["real_avg_forward_return_pct"]
                    if real_evaluations
                    else overall["avg_forward_return_pct"]
                )
                perf3.metric("Win rate", f"{win_rate:.1f}%")
                perf4.metric("Avg forward return", f"{avg_return:.2f}%")
                st.dataframe(performance_summary, width="stretch", hide_index=True)

    elif page == "Trends":
        with st.container(border=True):
            render_section_title("Sentiment Trend", "selected watchlist")
            chart = build_sentiment_chart(rolling)

            if chart is None:
                st.info("No rolling sentiment data available yet.")
            else:
                st.pyplot(chart, width="stretch")

        with st.container(border=True):
            render_section_title("Market Sentiment Index")
            market_chart = build_market_index_chart(market)

            if market_chart is None:
                st.info("No market sentiment index data available yet.")
            else:
                st.pyplot(market_chart, width="stretch")

        with st.container(border=True):
            render_section_title("Daily Sentiment Detail")
            st.dataframe(daily, width="stretch", hide_index=True)

    elif page == "Pipeline":
        render_metric_grid(latest_pipeline_metrics(pipeline_run_logs, health_alerts))

        with st.container(border=True):
            render_section_title("Operational Health")
            health_left, health_right = st.columns(2)
            with health_left:
                if health_alerts.empty:
                    st.success("No local health alerts recorded.")
                else:
                    open_alerts = filter_open_health_alerts(health_alerts)
                    if open_alerts.empty:
                        st.success("No open local health alerts.")
                    critical_count = (
                        int((open_alerts["severity"] == "critical").sum())
                        if not open_alerts.empty and "severity" in open_alerts.columns
                        else 0
                    )
                    warning_count = (
                        int((open_alerts["severity"] == "warning").sum())
                        if not open_alerts.empty and "severity" in open_alerts.columns
                        else 0
                    )
                    h1, h2, h3 = st.columns(3)
                    h1.metric("Open alerts", f"{len(open_alerts):,}")
                    h2.metric("Critical", f"{critical_count:,}")
                    h3.metric("Warnings", f"{warning_count:,}")
                    alert_columns = [
                        "detected_at_utc",
                        "severity",
                        "alert_type",
                        "message",
                        "run_id",
                        "status",
                    ]
                    st.dataframe(
                        select_existing_columns(
                            health_alerts.sort_values("detected_at_utc", ascending=False)
                            if "detected_at_utc" in health_alerts.columns
                            else health_alerts,
                            alert_columns,
                        ),
                        width="stretch",
                        hide_index=True,
                    )
            with health_right:
                render_section_title("Weekly Reports")
                if report_runs.empty:
                    st.info("No weekly reports generated yet.")
                else:
                    report_columns = [
                        "generated_at_utc",
                        "period_start",
                        "period_end",
                        "markdown_path",
                        "pdf_path",
                    ]
                    st.dataframe(
                        select_existing_columns(
                            report_runs.sort_values("generated_at_utc", ascending=False)
                            if "generated_at_utc" in report_runs.columns
                            else report_runs,
                            report_columns,
                        ),
                        width="stretch",
                        hide_index=True,
                    )

        with st.container(border=True):
            render_section_title("Pipeline Run Log")
            run_log_columns = [
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
            st.dataframe(
                select_existing_columns(
                    pipeline_run_logs.sort_values("started_at_utc", ascending=False)
                    if not pipeline_run_logs.empty and "started_at_utc" in pipeline_run_logs.columns
                    else pipeline_run_logs,
                    run_log_columns,
                ),
                width="stretch",
                hide_index=True,
            )

        with st.container(border=True):
            render_section_title("Pipeline Claim Audit")
            if audit.empty:
                st.info("No claim audit data available yet.")
            else:
                st.dataframe(audit, width="stretch", hide_index=True)

    elif page == "Data":
        with st.container(border=True):
            render_section_title("Latest Ticker Sentiment")
            latest_columns = [
                "ticker",
                "sentiment_label",
                "compound_score",
                "confidence",
                "source",
                "published_at_utc",
                "headline",
            ]
            st.dataframe(
                select_existing_columns(latest, latest_columns),
                width="stretch",
                hide_index=True,
            )

        with st.container(border=True):
            render_section_title("Signal Summary")
            if signals.empty:
                st.info("No signal data available yet.")
            else:
                signal_columns = [
                    "ticker",
                    "sentiment_date",
                    "headline_count",
                    "political_headline_count",
                    "financial_headline_count",
                    "sector",
                    "avg_compound_score",
                    "rolling_7_day_avg_compound_score",
                    "rolling_7_day_volume_weighted_sentiment_index",
                    "compound_score_zscore",
                    "signal_label",
                    "signal_score",
                    "recommendation",
                    "confidence_grade",
                    "source_count",
                    "source_diversity_score",
                    "sentiment_momentum",
                    "consensus_score",
                    "risk_score",
                    "opportunity_score",
                    "is_positive_sentiment_signal",
                    "is_negative_sentiment_signal",
                    "is_zscore_anomaly",
                    "rationale",
                ]

                st.dataframe(
                    select_existing_columns(signals, signal_columns),
                    width="stretch",
                    hide_index=True,
                )

        with st.container(border=True):
            render_section_title("Raw Daily Sentiment")
            st.dataframe(
                daily,
                width="stretch",
                hide_index=True,
            )


if __name__ == "__main__":
    main()
