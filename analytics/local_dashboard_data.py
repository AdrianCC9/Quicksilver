from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from config import settings
from storage.local_mysql_storage import LocalMySQLStorage


def load_local_dashboard_data(
    database_url: str | None = None,
) -> dict[str, pd.DataFrame]:
    storage = LocalMySQLStorage(database_url=database_url)
    try:
        storage.create_tables()
        scored = storage.fetch_dashboard_table("scored_headlines")
        insights = storage.fetch_dashboard_table("insights")
        price_quotes = storage.fetch_dashboard_table("price_quotes")
        runs = storage.fetch_dashboard_table("portfolio_runs")
        positions = storage.fetch_dashboard_table("portfolio_positions")
        trades = storage.fetch_dashboard_table("portfolio_trades")
        snapshots = storage.fetch_dashboard_table("portfolio_snapshots")
        run_logs = storage.fetch_dashboard_table("pipeline_run_logs")
        evaluations = storage.fetch_dashboard_table("insight_evaluations")
        health_alerts = storage.fetch_dashboard_table("health_alerts")
        report_runs = storage.fetch_dashboard_table("report_runs")
    finally:
        storage.close()

    sentiment_data = _build_sentiment_frames(scored, insights)
    sentiment_data.update(
        {
            "price_quotes": price_quotes,
            "portfolio_runs": runs,
            "portfolio_positions": positions,
            "portfolio_trades": trades,
            "portfolio_snapshots": snapshots,
            "pipeline_run_logs": run_logs,
            "insight_evaluations": evaluations,
            "health_alerts": health_alerts,
            "report_runs": report_runs,
            "performance_summary": _build_performance_summary(evaluations),
        }
    )
    return sentiment_data


def _build_sentiment_frames(
    scored: pd.DataFrame,
    insights: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if scored.empty:
        empty = pd.DataFrame()
        return {
            "latest": empty,
            "daily": empty,
            "rolling": empty,
            "signals": insights,
            "market": empty,
            "audit": empty,
        }

    scored = scored.copy()
    scored["published_at_utc"] = pd.to_datetime(scored["published_at_utc"], utc=True)
    scored["sentiment_date"] = scored["published_at_utc"].dt.date
    scored["is_positive"] = scored["sentiment_label"] == "positive"
    scored["is_negative"] = scored["sentiment_label"] == "negative"
    scored["is_neutral"] = scored["sentiment_label"] == "neutral"
    scored["absolute_sentiment_volume"] = scored["compound_score"].abs()
    scored["confidence_weighted_compound_score"] = (
        scored["compound_score"] * scored["confidence"]
    )
    scored["source_weight"] = scored["source_tier"].map({1: 1.2, 2: 1.0, 3: 0.82}).fillna(0.82)
    scored["source_weighted_compound_score"] = (
        scored["compound_score"] * scored["source_weight"]
    )

    latest = (
        scored.sort_values("published_at_utc", ascending=False)
        .groupby("ticker", as_index=False)
        .head(1)
        .sort_values("published_at_utc", ascending=False)
    )

    grouped = scored.groupby(["ticker", "sentiment_date"], as_index=False)
    daily = grouped.agg(
        headline_count=("headline", "count"),
        avg_compound_score=("compound_score", "mean"),
        avg_confidence=("confidence", "mean"),
        positive_headline_count=("is_positive", "sum"),
        neutral_headline_count=("is_neutral", "sum"),
        negative_headline_count=("is_negative", "sum"),
        political_headline_count=("category", lambda values: int((values == "political").sum())),
        financial_headline_count=("category", lambda values: int((values == "financial").sum())),
        avg_positive_score=("positive_score", "mean"),
        avg_neutral_score=("neutral_score", "mean"),
        avg_negative_score=("negative_score", "mean"),
        compound_score_sum=("compound_score", "sum"),
        absolute_sentiment_volume=("absolute_sentiment_volume", "sum"),
        confidence_weighted_compound_score=("confidence_weighted_compound_score", "mean"),
        source_weighted_compound_score=("source_weighted_compound_score", "mean"),
        first_headline_at_utc=("published_at_utc", "min"),
        latest_headline_at_utc=("published_at_utc", "max"),
    )
    daily["headline_volume_weighted_sentiment_index"] = daily[
        "avg_compound_score"
    ] * (1 + daily["headline_count"] / 20)
    daily = daily.sort_values(["ticker", "sentiment_date"])

    rolling = daily.copy()
    ticker_group = rolling.groupby("ticker", group_keys=False)
    rolling["rolling_7_day_avg_compound_score"] = ticker_group[
        "avg_compound_score"
    ].transform(lambda values: values.rolling(7, min_periods=1).mean())
    rolling["rolling_7_day_avg_headline_count"] = ticker_group[
        "headline_count"
    ].transform(lambda values: values.rolling(7, min_periods=1).mean())
    rolling["rolling_7_day_compound_score_stddev"] = ticker_group[
        "avg_compound_score"
    ].transform(lambda values: values.rolling(7, min_periods=2).std())
    rolling["rolling_7_day_compound_score_sum"] = ticker_group[
        "compound_score_sum"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())
    rolling["rolling_7_day_absolute_sentiment_volume"] = ticker_group[
        "absolute_sentiment_volume"
    ].transform(lambda values: values.rolling(7, min_periods=1).sum())
    rolling_weighted_sum = ticker_group["compound_score_sum"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    rolling_headline_sum = ticker_group["headline_count"].transform(
        lambda values: values.rolling(7, min_periods=1).sum()
    )
    rolling["rolling_7_day_volume_weighted_sentiment_index"] = (
        rolling_weighted_sum / rolling_headline_sum
    )

    signals = _build_signal_frame(rolling, insights)
    market = _build_market_frame(daily)
    audit = _build_audit_frame(scored)

    return {
        "latest": latest,
        "daily": daily.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "rolling": rolling.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "signals": signals.sort_values(["sentiment_date", "ticker"], ascending=[False, True]),
        "market": market.sort_values("sentiment_date", ascending=False),
        "audit": audit,
    }


def _build_signal_frame(rolling: pd.DataFrame, insights: pd.DataFrame) -> pd.DataFrame:
    signals = rolling.copy()
    signals["compound_score_zscore"] = (
        signals["avg_compound_score"] - signals["rolling_7_day_avg_compound_score"]
    ) / signals["rolling_7_day_compound_score_stddev"].replace(0, pd.NA)
    signals["compound_score_zscore"] = pd.to_numeric(
        signals["compound_score_zscore"],
        errors="coerce",
    ).fillna(0.0)
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

    if not insights.empty:
        insight_columns = [
            "ticker",
            "insight_date",
            "signal_label",
            "signal_score",
            "confidence",
            "sector",
            "source_count",
            "source_diversity_score",
            "sentiment_momentum",
            "consensus_score",
            "risk_score",
            "opportunity_score",
            "recommendation",
            "confidence_grade",
            "political_headline_count",
            "financial_headline_count",
            "category_mix",
            "rationale",
        ]
        available_columns = [
            column for column in insight_columns if column in insights.columns
        ]
        insight_subset = insights[available_columns].copy()
        insight_subset["insight_date"] = pd.to_datetime(
            insight_subset["insight_date"]
        ).dt.date
        signals = signals.merge(
            insight_subset,
            how="left",
            left_on=["ticker", "sentiment_date"],
            right_on=["ticker", "insight_date"],
            suffixes=("", "_insight"),
        )

    return signals


def _build_market_frame(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for sentiment_date, group in daily.groupby("sentiment_date"):
        headline_count = int(group["headline_count"].sum())
        volume_weighted = (
            float((group["avg_compound_score"] * group["headline_count"]).sum())
            / headline_count
            if headline_count
            else 0.0
        )
        rows.append(
            {
                "sentiment_date": sentiment_date,
                "ticker_count": group["ticker"].nunique(),
                "headline_count": headline_count,
                "equal_weight_sentiment_index": group["avg_compound_score"].mean(),
                "volume_weighted_sentiment_index": volume_weighted,
                "confidence_volume_weighted_sentiment_index": (
                    group["confidence_weighted_compound_score"] * group["headline_count"]
                ).sum()
                / headline_count,
                "source_volume_weighted_sentiment_index": (
                    group["source_weighted_compound_score"] * group["headline_count"]
                ).sum()
                / headline_count,
            }
        )

    market = pd.DataFrame(rows).sort_values("sentiment_date")
    market["rolling_7_day_volume_weighted_sentiment_index"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=1).mean()
    market["rolling_7_day_volume_weighted_sentiment_stddev"] = market[
        "volume_weighted_sentiment_index"
    ].rolling(7, min_periods=2).std()
    return market


def _build_audit_frame(scored: pd.DataFrame) -> pd.DataFrame:
    first_at = scored["published_at_utc"].min()
    latest_at = scored["published_at_utc"].max()
    daily_counts = scored.groupby(scored["published_at_utc"].dt.date).size()
    coverage_days = int((latest_at.date() - first_at.date()).days + 1)

    return pd.DataFrame(
        [
            {
                "first_scored_headline_at_utc": first_at,
                "latest_scored_headline_at_utc": latest_at,
                "coverage_days": coverage_days,
                "total_scored_headlines": len(scored),
                "tracked_ticker_count": scored["ticker"].nunique(),
                "avg_scored_headlines_per_day": daily_counts.mean(),
                "max_scored_headlines_in_one_day": int(daily_counts.max()),
                "days_with_500_plus_scored_headlines": int((daily_counts >= 500).sum()),
                "has_50_plus_tickers": scored["ticker"].nunique() >= 50,
                "has_2_plus_years": coverage_days >= 730,
                "has_500_plus_daily_headlines": int(daily_counts.max()) >= 500,
                "loaded_at_utc": datetime.now(timezone.utc),
            }
        ]
    )


def _build_performance_summary(evaluations: pd.DataFrame) -> pd.DataFrame:
    if evaluations.empty:
        return pd.DataFrame()

    working = evaluations.copy()
    working["direction_correct"] = working["direction_correct"].astype(int)
    working["is_real_market_data"] = working["is_real_market_data"].astype(int)

    def summarize(group: pd.DataFrame) -> pd.Series:
        real_group = group[group["is_real_market_data"] == 1]
        real_count = len(real_group)
        return pd.Series(
            {
                "evaluated_insights": len(group),
                "real_market_evaluations": int(group["is_real_market_data"].sum()),
                "synthetic_evaluations": int((group["is_real_market_data"] == 0).sum()),
                "win_rate_pct": group["direction_correct"].mean() * 100,
                "real_win_rate_pct": (
                    real_group["direction_correct"].mean() * 100
                    if real_count
                    else 0.0
                ),
                "avg_forward_return_pct": group["forward_return_pct"].mean(),
                "real_avg_forward_return_pct": (
                    real_group["forward_return_pct"].mean()
                    if real_count
                    else 0.0
                ),
                "median_forward_return_pct": group["forward_return_pct"].median(),
                "best_forward_return_pct": group["forward_return_pct"].max(),
                "worst_forward_return_pct": group["forward_return_pct"].min(),
                "matured_count": int((group["evaluation_status"] == "matured").sum()),
            }
        )

    overall = summarize(working).to_frame().T
    overall.insert(0, "segment", "overall")

    by_signal = _summarize_segments(working, "signal_label", summarize, "signal")

    if "recommendation" in working.columns:
        by_recommendation = _summarize_segments(
            working,
            "recommendation",
            summarize,
            "recommendation",
        )
    else:
        by_recommendation = pd.DataFrame()

    return pd.concat(
        [overall, by_signal, by_recommendation],
        ignore_index=True,
    )


def _summarize_segments(
    dataframe: pd.DataFrame,
    column: str,
    summarize,
    prefix: str,
) -> pd.DataFrame:
    rows: list[pd.Series] = []
    for value, group in dataframe.groupby(column):
        summary = summarize(group)
        summary["segment"] = f"{prefix}:{value}"
        rows.append(summary)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)
