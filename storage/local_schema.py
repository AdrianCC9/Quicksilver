from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)


@dataclass(slots=True)
class LocalTables:
    raw_headlines: Table
    scored_headlines: Table
    insights: Table
    pipeline_run_logs: Table
    insight_evaluations: Table
    price_quotes: Table
    portfolio_runs: Table
    portfolio_positions: Table
    portfolio_trades: Table
    portfolio_snapshots: Table
    health_alerts: Table
    report_runs: Table


def define_local_tables(metadata: MetaData) -> LocalTables:
    raw_headlines = Table(
        "raw_headlines",
        metadata,
        Column("raw_headline_id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(16), nullable=False),
        Column("headline", Text, nullable=False),
        Column("source", String(255), nullable=False),
        Column("url", Text),
        Column("published_at_utc", DateTime(timezone=True), nullable=False),
        Column("summary", Text),
        Column("category", String(32), nullable=False, default="financial"),
        Column("topic", String(128)),
        Column("industry", String(128)),
        Column("content_hash", String(64), nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint("content_hash", name="uq_raw_headlines_content_hash"),
        Index("ix_raw_headlines_ticker_published", "ticker", "published_at_utc"),
        Index("ix_raw_headlines_category_topic", "category", "topic"),
    )

    scored_headlines = Table(
        "scored_headlines",
        metadata,
        Column("scored_headline_id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(16), nullable=False),
        Column("headline", Text, nullable=False),
        Column("source", String(255), nullable=False),
        Column("url", Text),
        Column("published_at_utc", DateTime(timezone=True), nullable=False),
        Column("sentiment_label", String(32), nullable=False),
        Column("positive_score", Float, nullable=False),
        Column("neutral_score", Float, nullable=False),
        Column("negative_score", Float, nullable=False),
        Column("compound_score", Float, nullable=False),
        Column("confidence", Float, nullable=False),
        Column("headline_age_hours", Float, nullable=False),
        Column("source_tier", Integer, nullable=False),
        Column("summary", Text),
        Column("category", String(32), nullable=False, default="financial"),
        Column("topic", String(128)),
        Column("industry", String(128)),
        Column("content_hash", String(64), nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint("content_hash", name="uq_scored_headlines_content_hash"),
        Index("ix_scored_headlines_ticker_published", "ticker", "published_at_utc"),
        Index("ix_scored_headlines_category_topic", "category", "topic"),
    )

    insights = Table(
        "insights",
        metadata,
        Column("insight_id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(16), nullable=False),
        Column("insight_date", Date, nullable=False),
        Column("generated_at_utc", DateTime(timezone=True), nullable=False),
        Column("signal_label", String(32), nullable=False),
        Column("signal_score", Float, nullable=False),
        Column("confidence", Float, nullable=False),
        Column("headline_count", Integer, nullable=False),
        Column("political_headline_count", Integer, nullable=False),
        Column("financial_headline_count", Integer, nullable=False),
        Column("sector", String(128)),
        Column("source_count", Integer),
        Column("source_diversity_score", Float),
        Column("sentiment_momentum", Float),
        Column("consensus_score", Float),
        Column("risk_score", Float),
        Column("opportunity_score", Float),
        Column("recommendation", String(32)),
        Column("confidence_grade", String(8)),
        Column("category_mix", String(255), nullable=False),
        Column("rationale", Text, nullable=False),
        Column("horizon_days", Integer, nullable=False, default=5),
        Column("source_headline_hashes", Text, nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint("ticker", "insight_date", name="uq_insights_ticker_date"),
        Index("ix_insights_date_score", "insight_date", "signal_score"),
    )

    pipeline_run_logs = Table(
        "pipeline_run_logs",
        metadata,
        Column("pipeline_run_log_id", Integer, primary_key=True, autoincrement=True),
        Column("run_id", String(64), nullable=False, unique=True),
        Column("run_type", String(64), nullable=False),
        Column("started_at_utc", DateTime(timezone=True), nullable=False),
        Column("finished_at_utc", DateTime(timezone=True)),
        Column("status", String(32), nullable=False),
        Column("ticker_count", Integer),
        Column("raw_headlines_collected", Integer),
        Column("scored_headlines", Integer),
        Column("insights_generated", Integer),
        Column("trades_executed", Integer),
        Column("error_message", Text),
        Column("summary_json", Text, nullable=False),
        Index("ix_pipeline_run_logs_started", "started_at_utc"),
    )

    insight_evaluations = Table(
        "insight_evaluations",
        metadata,
        Column("insight_evaluation_id", Integer, primary_key=True, autoincrement=True),
        Column("insight_id", Integer, ForeignKey("insights.insight_id"), nullable=False),
        Column("ticker", String(16), nullable=False),
        Column("insight_date", Date, nullable=False),
        Column("evaluation_date", Date, nullable=False),
        Column("evaluated_at_utc", DateTime(timezone=True), nullable=False),
        Column("signal_label", String(32), nullable=False),
        Column("recommendation", String(32)),
        Column("signal_score", Float, nullable=False),
        Column("horizon_days", Integer, nullable=False),
        Column("entry_quote_date", Date, nullable=False),
        Column("current_quote_date", Date, nullable=False),
        Column("entry_price_usd", Float, nullable=False),
        Column("current_price_usd", Float, nullable=False),
        Column("forward_return_pct", Float, nullable=False),
        Column("direction_correct", Integer, nullable=False),
        Column("is_real_market_data", Integer, nullable=False),
        Column("evaluation_status", String(32), nullable=False),
        Column("data_source", String(128), nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint(
            "insight_id",
            "evaluation_date",
            name="uq_insight_evaluations_insight_date",
        ),
        Index("ix_insight_evaluations_ticker_date", "ticker", "evaluation_date"),
    )

    price_quotes = Table(
        "price_quotes",
        metadata,
        Column("price_quote_id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(16), nullable=False),
        Column("quote_date", Date, nullable=False),
        Column("close_price_usd", Float, nullable=False),
        Column("data_source", String(64), nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint("ticker", "quote_date", name="uq_price_quotes_ticker_date"),
        Index("ix_price_quotes_ticker_date", "ticker", "quote_date"),
    )

    portfolio_runs = Table(
        "portfolio_runs",
        metadata,
        Column("portfolio_run_id", Integer, primary_key=True, autoincrement=True),
        Column("run_name", String(128), nullable=False, unique=True),
        Column("starting_cash_cad", Float, nullable=False),
        Column("cash_cad", Float, nullable=False),
        Column("status", String(32), nullable=False, default="active"),
        Column("created_at_utc", DateTime(timezone=True), nullable=False),
        Column("updated_at_utc", DateTime(timezone=True), nullable=False),
    )

    portfolio_positions = Table(
        "portfolio_positions",
        metadata,
        Column("position_id", Integer, primary_key=True, autoincrement=True),
        Column(
            "portfolio_run_id",
            Integer,
            ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        Column("ticker", String(16), nullable=False),
        Column("quantity", Float, nullable=False),
        Column("avg_cost_cad", Float, nullable=False),
        Column("last_price_cad", Float, nullable=False),
        Column("market_value_cad", Float, nullable=False),
        Column("unrealized_pnl_cad", Float, nullable=False),
        Column("updated_at_utc", DateTime(timezone=True), nullable=False),
        UniqueConstraint(
            "portfolio_run_id",
            "ticker",
            name="uq_portfolio_positions_run_ticker",
        ),
    )

    portfolio_trades = Table(
        "portfolio_trades",
        metadata,
        Column("trade_id", Integer, primary_key=True, autoincrement=True),
        Column(
            "portfolio_run_id",
            Integer,
            ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        Column("insight_id", Integer, ForeignKey("insights.insight_id")),
        Column("traded_at_utc", DateTime(timezone=True), nullable=False),
        Column("ticker", String(16), nullable=False),
        Column("side", String(8), nullable=False),
        Column("quantity", Float, nullable=False),
        Column("price_cad", Float, nullable=False),
        Column("gross_cad", Float, nullable=False),
        Column("fee_cad", Float, nullable=False, default=0.0),
        Column("reason", Text, nullable=False),
        Index("ix_portfolio_trades_run_time", "portfolio_run_id", "traded_at_utc"),
    )

    portfolio_snapshots = Table(
        "portfolio_snapshots",
        metadata,
        Column("snapshot_id", Integer, primary_key=True, autoincrement=True),
        Column(
            "portfolio_run_id",
            Integer,
            ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        Column("snapshot_date", Date, nullable=False),
        Column("cash_cad", Float, nullable=False),
        Column("positions_value_cad", Float, nullable=False),
        Column("total_equity_cad", Float, nullable=False),
        Column("cumulative_return_pct", Float, nullable=False),
        Column("data_source", String(64), nullable=False),
        Column("inserted_at_utc", DateTime(timezone=True), server_default=func.now()),
        UniqueConstraint(
            "portfolio_run_id",
            "snapshot_date",
            name="uq_portfolio_snapshots_run_date",
        ),
        Index("ix_portfolio_snapshots_run_date", "portfolio_run_id", "snapshot_date"),
    )

    health_alerts = Table(
        "health_alerts",
        metadata,
        Column("health_alert_id", Integer, primary_key=True, autoincrement=True),
        Column("alert_key", String(128), nullable=False),
        Column("run_id", String(64)),
        Column("severity", String(16), nullable=False),
        Column("alert_type", String(64), nullable=False),
        Column("message", Text, nullable=False),
        Column("details_json", Text, nullable=False),
        Column("detected_at_utc", DateTime(timezone=True), nullable=False),
        Column("status", String(32), nullable=False, default="open"),
        UniqueConstraint("alert_key", name="uq_health_alerts_alert_key"),
        Index("ix_health_alerts_detected", "detected_at_utc"),
        Index("ix_health_alerts_status_severity", "status", "severity"),
    )

    report_runs = Table(
        "report_runs",
        metadata,
        Column("report_run_id", Integer, primary_key=True, autoincrement=True),
        Column("report_name", String(128), nullable=False),
        Column("period_start", Date, nullable=False),
        Column("period_end", Date, nullable=False),
        Column("generated_at_utc", DateTime(timezone=True), nullable=False),
        Column("output_dir", Text, nullable=False),
        Column("markdown_path", Text),
        Column("pdf_path", Text),
        Column("metrics_json", Text, nullable=False),
        UniqueConstraint(
            "report_name",
            "period_start",
            "period_end",
            name="uq_report_runs_name_period",
        ),
        Index("ix_report_runs_generated", "generated_at_utc"),
    )

    return LocalTables(
        raw_headlines=raw_headlines,
        scored_headlines=scored_headlines,
        insights=insights,
        pipeline_run_logs=pipeline_run_logs,
        insight_evaluations=insight_evaluations,
        price_quotes=price_quotes,
        portfolio_runs=portfolio_runs,
        portfolio_positions=portfolio_positions,
        portfolio_trades=portfolio_trades,
        portfolio_snapshots=portfolio_snapshots,
        health_alerts=health_alerts,
        report_runs=report_runs,
    )
