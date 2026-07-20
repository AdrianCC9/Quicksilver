from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_local_mysql_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "raw_headlines",
        sa.Column("raw_headline_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text()),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("summary", sa.Text()),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("topic", sa.String(length=128)),
        sa.Column("industry", sa.String(length=128)),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("content_hash", name="uq_raw_headlines_content_hash"),
    )
    op.create_index(
        "ix_raw_headlines_ticker_published",
        "raw_headlines",
        ["ticker", "published_at_utc"],
    )
    op.create_index("ix_raw_headlines_category_topic", "raw_headlines", ["category", "topic"])

    op.create_table(
        "scored_headlines",
        sa.Column("scored_headline_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text()),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sentiment_label", sa.String(length=32), nullable=False),
        sa.Column("positive_score", sa.Float(), nullable=False),
        sa.Column("neutral_score", sa.Float(), nullable=False),
        sa.Column("negative_score", sa.Float(), nullable=False),
        sa.Column("compound_score", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("headline_age_hours", sa.Float(), nullable=False),
        sa.Column("source_tier", sa.Integer(), nullable=False),
        sa.Column("summary", sa.Text()),
        sa.Column("category", sa.String(length=32), nullable=False),
        sa.Column("topic", sa.String(length=128)),
        sa.Column("industry", sa.String(length=128)),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("content_hash", name="uq_scored_headlines_content_hash"),
    )
    op.create_index(
        "ix_scored_headlines_ticker_published",
        "scored_headlines",
        ["ticker", "published_at_utc"],
    )
    op.create_index(
        "ix_scored_headlines_category_topic",
        "scored_headlines",
        ["category", "topic"],
    )

    op.create_table(
        "insights",
        sa.Column("insight_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("insight_date", sa.Date(), nullable=False),
        sa.Column("generated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_label", sa.String(length=32), nullable=False),
        sa.Column("signal_score", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("headline_count", sa.Integer(), nullable=False),
        sa.Column("political_headline_count", sa.Integer(), nullable=False),
        sa.Column("financial_headline_count", sa.Integer(), nullable=False),
        sa.Column("sector", sa.String(length=128)),
        sa.Column("source_count", sa.Integer()),
        sa.Column("source_diversity_score", sa.Float()),
        sa.Column("sentiment_momentum", sa.Float()),
        sa.Column("consensus_score", sa.Float()),
        sa.Column("risk_score", sa.Float()),
        sa.Column("opportunity_score", sa.Float()),
        sa.Column("recommendation", sa.String(length=32)),
        sa.Column("confidence_grade", sa.String(length=8)),
        sa.Column("category_mix", sa.String(length=255), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False),
        sa.Column("source_headline_hashes", sa.Text(), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("ticker", "insight_date", name="uq_insights_ticker_date"),
    )
    op.create_index("ix_insights_date_score", "insights", ["insight_date", "signal_score"])

    op.create_table(
        "pipeline_run_logs",
        sa.Column("pipeline_run_log_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False, unique=True),
        sa.Column("run_type", sa.String(length=64), nullable=False),
        sa.Column("started_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at_utc", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("ticker_count", sa.Integer()),
        sa.Column("raw_headlines_collected", sa.Integer()),
        sa.Column("scored_headlines", sa.Integer()),
        sa.Column("insights_generated", sa.Integer()),
        sa.Column("trades_executed", sa.Integer()),
        sa.Column("error_message", sa.Text()),
        sa.Column("summary_json", sa.Text(), nullable=False),
    )
    op.create_index("ix_pipeline_run_logs_started", "pipeline_run_logs", ["started_at_utc"])

    op.create_table(
        "insight_evaluations",
        sa.Column("insight_evaluation_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("insight_id", sa.Integer(), sa.ForeignKey("insights.insight_id"), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("insight_date", sa.Date(), nullable=False),
        sa.Column("evaluation_date", sa.Date(), nullable=False),
        sa.Column("evaluated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_label", sa.String(length=32), nullable=False),
        sa.Column("recommendation", sa.String(length=32)),
        sa.Column("signal_score", sa.Float(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False),
        sa.Column("entry_quote_date", sa.Date(), nullable=False),
        sa.Column("current_quote_date", sa.Date(), nullable=False),
        sa.Column("entry_price_usd", sa.Float(), nullable=False),
        sa.Column("current_price_usd", sa.Float(), nullable=False),
        sa.Column("forward_return_pct", sa.Float(), nullable=False),
        sa.Column("direction_correct", sa.Integer(), nullable=False),
        sa.Column("is_real_market_data", sa.Integer(), nullable=False),
        sa.Column("evaluation_status", sa.String(length=32), nullable=False),
        sa.Column("data_source", sa.String(length=128), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "insight_id",
            "evaluation_date",
            name="uq_insight_evaluations_insight_date",
        ),
    )
    op.create_index(
        "ix_insight_evaluations_ticker_date",
        "insight_evaluations",
        ["ticker", "evaluation_date"],
    )

    op.create_table(
        "price_quotes",
        sa.Column("price_quote_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("quote_date", sa.Date(), nullable=False),
        sa.Column("close_price_usd", sa.Float(), nullable=False),
        sa.Column("data_source", sa.String(length=64), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("ticker", "quote_date", name="uq_price_quotes_ticker_date"),
    )
    op.create_index("ix_price_quotes_ticker_date", "price_quotes", ["ticker", "quote_date"])

    op.create_table(
        "portfolio_runs",
        sa.Column("portfolio_run_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_name", sa.String(length=128), nullable=False, unique=True),
        sa.Column("starting_cash_cad", sa.Float(), nullable=False),
        sa.Column("cash_cad", sa.Float(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "portfolio_positions",
        sa.Column("position_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "portfolio_run_id",
            sa.Integer(),
            sa.ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("avg_cost_cad", sa.Float(), nullable=False),
        sa.Column("last_price_cad", sa.Float(), nullable=False),
        sa.Column("market_value_cad", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl_cad", sa.Float(), nullable=False),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "portfolio_run_id",
            "ticker",
            name="uq_portfolio_positions_run_ticker",
        ),
    )

    op.create_table(
        "portfolio_trades",
        sa.Column("trade_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "portfolio_run_id",
            sa.Integer(),
            sa.ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        sa.Column("insight_id", sa.Integer(), sa.ForeignKey("insights.insight_id")),
        sa.Column("traded_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price_cad", sa.Float(), nullable=False),
        sa.Column("gross_cad", sa.Float(), nullable=False),
        sa.Column("fee_cad", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
    )
    op.create_index(
        "ix_portfolio_trades_run_time",
        "portfolio_trades",
        ["portfolio_run_id", "traded_at_utc"],
    )

    op.create_table(
        "portfolio_snapshots",
        sa.Column("snapshot_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "portfolio_run_id",
            sa.Integer(),
            sa.ForeignKey("portfolio_runs.portfolio_run_id"),
            nullable=False,
        ),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("cash_cad", sa.Float(), nullable=False),
        sa.Column("positions_value_cad", sa.Float(), nullable=False),
        sa.Column("total_equity_cad", sa.Float(), nullable=False),
        sa.Column("cumulative_return_pct", sa.Float(), nullable=False),
        sa.Column("data_source", sa.String(length=64), nullable=False),
        sa.Column("inserted_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "portfolio_run_id",
            "snapshot_date",
            name="uq_portfolio_snapshots_run_date",
        ),
    )
    op.create_index(
        "ix_portfolio_snapshots_run_date",
        "portfolio_snapshots",
        ["portfolio_run_id", "snapshot_date"],
    )

    op.create_table(
        "health_alerts",
        sa.Column("health_alert_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("alert_key", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=64)),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("alert_type", sa.String(length=64), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=False),
        sa.Column("detected_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.UniqueConstraint("alert_key", name="uq_health_alerts_alert_key"),
    )
    op.create_index("ix_health_alerts_detected", "health_alerts", ["detected_at_utc"])
    op.create_index(
        "ix_health_alerts_status_severity",
        "health_alerts",
        ["status", "severity"],
    )

    op.create_table(
        "report_runs",
        sa.Column("report_run_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("report_name", sa.String(length=128), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("generated_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("output_dir", sa.Text(), nullable=False),
        sa.Column("markdown_path", sa.Text()),
        sa.Column("pdf_path", sa.Text()),
        sa.Column("metrics_json", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "report_name",
            "period_start",
            "period_end",
            name="uq_report_runs_name_period",
        ),
    )
    op.create_index("ix_report_runs_generated", "report_runs", ["generated_at_utc"])


def downgrade() -> None:
    for table_name in (
        "report_runs",
        "health_alerts",
        "portfolio_snapshots",
        "portfolio_trades",
        "portfolio_positions",
        "portfolio_runs",
        "price_quotes",
        "insight_evaluations",
        "pipeline_run_logs",
        "insights",
        "scored_headlines",
        "raw_headlines",
    ):
        op.drop_table(table_name)
