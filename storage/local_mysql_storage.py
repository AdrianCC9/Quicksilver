from __future__ import annotations

from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
import json
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import (
    Column,
    MetaData,
    Table,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError

from config import settings
from models.insight import Insight
from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline
from storage.local_schema import define_local_tables
from transformations.headline_normalizer import HeadlineNormalizer


class LocalMySQLStorage:
    """
    Local relational storage for Quicksilver.

    The production-local target is MySQL, configured through DATABASE_URL.
    SQLAlchemy keeps the implementation portable enough for fast SQLite tests.
    """

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or settings.database_url
        self.engine = create_engine(self.database_url, pool_pre_ping=True, future=True)
        self.metadata = MetaData()
        self._normalizer = HeadlineNormalizer()
        self._define_tables()

    def _define_tables(self) -> None:
        tables = define_local_tables(self.metadata)
        for table_field in fields(tables):
            setattr(self, table_field.name, getattr(tables, table_field.name))

    def create_tables(self) -> None:
        self.metadata.create_all(self.engine)
        self._ensure_known_columns()

    def save_raw_headline(self, headline: RawHeadline) -> None:
        self.save_raw_headlines([headline])

    def save_raw_headlines(self, headlines: list[RawHeadline]) -> None:
        if not headlines:
            return

        rows = [
            {
                "ticker": headline.ticker,
                "headline": headline.headline,
                "source": headline.source,
                "url": headline.url,
                "published_at_utc": headline.published_at_utc,
                "summary": headline.summary,
                "category": headline.category,
                "topic": headline.topic,
                "industry": headline.industry,
                "content_hash": self._raw_content_hash(headline),
            }
            for headline in headlines
        ]

        with self.engine.begin() as connection:
            self._insert_ignore(
                connection,
                self.raw_headlines,
                rows,
                conflict_columns=["content_hash"],
            )

    def save_scored_headline(self, headline: ScoredHeadline) -> None:
        self.save_scored_headlines([headline])

    def save_scored_headlines(self, headlines: list[ScoredHeadline]) -> None:
        if not headlines:
            return

        rows = [
            {
                "ticker": headline.ticker,
                "headline": headline.headline,
                "source": headline.source,
                "url": headline.url,
                "published_at_utc": headline.published_at_utc,
                "sentiment_label": headline.sentiment_label,
                "positive_score": headline.positive_score,
                "neutral_score": headline.neutral_score,
                "negative_score": headline.negative_score,
                "compound_score": headline.compound_score,
                "confidence": headline.confidence,
                "headline_age_hours": headline.headline_age_hours,
                "source_tier": headline.source_tier,
                "summary": headline.summary,
                "category": headline.category,
                "topic": headline.topic,
                "industry": headline.industry,
                "content_hash": self._scored_content_hash(headline),
            }
            for headline in headlines
        ]

        with self.engine.begin() as connection:
            self._insert_ignore(
                connection,
                self.scored_headlines,
                rows,
                conflict_columns=["content_hash"],
            )

    def save_insights(self, insights: list[Insight]) -> None:
        if not insights:
            return

        rows = [
            {
                "ticker": insight.ticker,
                "insight_date": insight.insight_date,
                "generated_at_utc": insight.generated_at_utc,
                "signal_label": insight.signal_label,
                "signal_score": insight.signal_score,
                "confidence": insight.confidence,
                "headline_count": insight.headline_count,
                "political_headline_count": insight.political_headline_count,
                "financial_headline_count": insight.financial_headline_count,
                "sector": insight.sector,
                "source_count": insight.source_count,
                "source_diversity_score": insight.source_diversity_score,
                "sentiment_momentum": insight.sentiment_momentum,
                "consensus_score": insight.consensus_score,
                "risk_score": insight.risk_score,
                "opportunity_score": insight.opportunity_score,
                "recommendation": insight.recommendation,
                "confidence_grade": insight.confidence_grade,
                "category_mix": insight.category_mix,
                "rationale": insight.rationale,
                "horizon_days": insight.horizon_days,
                "source_headline_hashes": json.dumps(insight.source_headline_hashes),
            }
            for insight in insights
        ]

        update_columns = [
            "generated_at_utc",
            "signal_label",
            "signal_score",
            "confidence",
            "headline_count",
            "political_headline_count",
            "financial_headline_count",
            "sector",
            "source_count",
            "source_diversity_score",
            "sentiment_momentum",
            "consensus_score",
            "risk_score",
            "opportunity_score",
            "recommendation",
            "confidence_grade",
            "category_mix",
            "rationale",
            "horizon_days",
            "source_headline_hashes",
        ]

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.insights,
                rows,
                conflict_columns=["ticker", "insight_date"],
                update_columns=update_columns,
            )

    def save_price_quotes(self, quotes: list[dict[str, Any]]) -> None:
        if not quotes:
            return

        rows = [
            {
                "ticker": quote["ticker"],
                "quote_date": quote["quote_date"],
                "close_price_usd": quote["close_price_usd"],
                "data_source": quote["data_source"],
            }
            for quote in quotes
        ]

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.price_quotes,
                rows,
                conflict_columns=["ticker", "quote_date"],
                update_columns=["close_price_usd", "data_source"],
            )

    def fetch_latest_price_quote(
        self,
        ticker: str,
        as_of_date: date,
        max_age_days: int = 14,
        real_only: bool = True,
    ) -> dict[str, Any] | None:
        query = (
            select(self.price_quotes)
            .where(self.price_quotes.c.ticker == ticker.upper())
            .where(self.price_quotes.c.quote_date <= as_of_date)
            .where(
                self.price_quotes.c.quote_date
                >= as_of_date - timedelta(days=max_age_days)
            )
        )
        if real_only:
            query = query.where(self.price_quotes.c.data_source != "synthetic")
        query = query.order_by(self.price_quotes.c.quote_date.desc()).limit(1)

        with self.engine.begin() as connection:
            row = connection.execute(query).mappings().first()
            return dict(row) if row else None

    def fetch_recent_scored_headlines(
        self,
        since_utc: datetime,
        tickers: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        query = select(self.scored_headlines).where(
            self.scored_headlines.c.published_at_utc >= since_utc
        )
        ticker_list = list(tickers or [])
        if ticker_list:
            query = query.where(self.scored_headlines.c.ticker.in_(ticker_list))
        query = query.order_by(self.scored_headlines.c.published_at_utc.desc())
        return pd.read_sql_query(query, self.engine)

    def fetch_latest_insights(self, as_of_date: date | None = None) -> pd.DataFrame:
        selected_date = as_of_date or date.today()
        query = (
            select(self.insights)
            .where(self.insights.c.insight_date <= selected_date)
            .order_by(
                self.insights.c.insight_date.desc(),
                self.insights.c.signal_score.desc(),
            )
        )
        return pd.read_sql_query(query, self.engine)

    def fetch_dashboard_table(self, table_name: str) -> pd.DataFrame:
        if table_name not in {
            "raw_headlines",
            "scored_headlines",
            "insights",
            "price_quotes",
            "portfolio_runs",
            "portfolio_positions",
            "portfolio_trades",
            "portfolio_snapshots",
            "pipeline_run_logs",
            "insight_evaluations",
            "health_alerts",
            "report_runs",
        }:
            raise ValueError(f"Unsupported dashboard table: {table_name}")

        table = getattr(self, table_name)
        return pd.read_sql_query(select(table), self.engine)

    def get_or_create_portfolio_run(
        self,
        run_name: str,
        starting_cash_cad: float,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)

        with self.engine.begin() as connection:
            row = connection.execute(
                select(self.portfolio_runs).where(
                    self.portfolio_runs.c.run_name == run_name
                )
            ).mappings().first()
            if row:
                return dict(row)

            result = connection.execute(
                self.portfolio_runs.insert().values(
                    run_name=run_name,
                    starting_cash_cad=starting_cash_cad,
                    cash_cad=starting_cash_cad,
                    status="active",
                    created_at_utc=now,
                    updated_at_utc=now,
                )
            )
            inserted_id = result.inserted_primary_key[0]
            row = connection.execute(
                select(self.portfolio_runs).where(
                    self.portfolio_runs.c.portfolio_run_id == inserted_id
                )
            ).mappings().one()
            return dict(row)

    def save_pipeline_run_log(
        self,
        run_id: str,
        run_type: str,
        started_at_utc: datetime,
        finished_at_utc: datetime | None,
        status: str,
        summary: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        row = {
            "run_id": run_id,
            "run_type": run_type,
            "started_at_utc": started_at_utc,
            "finished_at_utc": finished_at_utc,
            "status": status,
            "ticker_count": summary.get("tickers"),
            "raw_headlines_collected": summary.get("raw_headlines_collected"),
            "scored_headlines": summary.get("scored_headlines_saved_attempted"),
            "insights_generated": summary.get("insights_generated"),
            "trades_executed": (
                (summary.get("simulation") or {}).get("trades_executed")
                if isinstance(summary.get("simulation"), dict)
                else None
            ),
            "error_message": error_message,
            "summary_json": json.dumps(summary, default=str),
        }

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.pipeline_run_logs,
                [row],
                conflict_columns=["run_id"],
                update_columns=[
                    "finished_at_utc",
                    "status",
                    "ticker_count",
                    "raw_headlines_collected",
                    "scored_headlines",
                    "insights_generated",
                    "trades_executed",
                    "error_message",
                    "summary_json",
                ],
            )

    def save_insight_evaluations(self, evaluations: list[dict[str, Any]]) -> None:
        if not evaluations:
            return

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.insight_evaluations,
                evaluations,
                conflict_columns=["insight_id", "evaluation_date"],
                update_columns=[
                    "evaluated_at_utc",
                    "signal_label",
                    "recommendation",
                    "signal_score",
                    "horizon_days",
                    "entry_quote_date",
                    "current_quote_date",
                    "entry_price_usd",
                    "current_price_usd",
                    "forward_return_pct",
                    "direction_correct",
                    "is_real_market_data",
                    "evaluation_status",
                    "data_source",
                ],
            )

    def save_health_alerts(self, alerts: list[dict[str, Any]]) -> None:
        if not alerts:
            return

        rows = [
            {
                "alert_key": alert["alert_key"],
                "run_id": alert.get("run_id"),
                "severity": alert["severity"],
                "alert_type": alert["alert_type"],
                "message": alert["message"],
                "details_json": json.dumps(alert.get("details", {}), default=str),
                "detected_at_utc": alert.get(
                    "detected_at_utc",
                    datetime.now(timezone.utc),
                ),
                "status": alert.get("status", "open"),
            }
            for alert in alerts
        ]

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.health_alerts,
                rows,
                conflict_columns=["alert_key"],
                update_columns=[
                    "run_id",
                    "severity",
                    "alert_type",
                    "message",
                    "details_json",
                    "detected_at_utc",
                    "status",
                ],
            )

    def resolve_health_alerts(self, alert_types: Iterable[str]) -> None:
        alert_type_list = list(alert_types)
        if not alert_type_list:
            return

        with self.engine.begin() as connection:
            connection.execute(
                self.health_alerts.update()
                .where(self.health_alerts.c.status == "open")
                .where(self.health_alerts.c.alert_type.in_(alert_type_list))
                .values(status="resolved")
            )

    def save_report_run(
        self,
        report_name: str,
        period_start: date,
        period_end: date,
        output_dir: str,
        markdown_path: str | None,
        pdf_path: str | None,
        metrics: dict[str, Any],
    ) -> None:
        row = {
            "report_name": report_name,
            "period_start": period_start,
            "period_end": period_end,
            "generated_at_utc": datetime.now(timezone.utc),
            "output_dir": output_dir,
            "markdown_path": markdown_path,
            "pdf_path": pdf_path,
            "metrics_json": json.dumps(metrics, default=str),
        }

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.report_runs,
                [row],
                conflict_columns=["report_name", "period_start", "period_end"],
                update_columns=[
                    "generated_at_utc",
                    "output_dir",
                    "markdown_path",
                    "pdf_path",
                    "metrics_json",
                ],
            )

    def fetch_portfolio_run(self, portfolio_run_id: int) -> dict[str, Any]:
        with self.engine.begin() as connection:
            row = connection.execute(
                select(self.portfolio_runs).where(
                    self.portfolio_runs.c.portfolio_run_id == portfolio_run_id
                )
            ).mappings().one()
            return dict(row)

    def update_portfolio_cash(self, portfolio_run_id: int, cash_cad: float) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                self.portfolio_runs.update()
                .where(self.portfolio_runs.c.portfolio_run_id == portfolio_run_id)
                .values(
                    cash_cad=cash_cad,
                    updated_at_utc=datetime.now(timezone.utc),
                )
            )

    def fetch_positions(self, portfolio_run_id: int) -> pd.DataFrame:
        query = select(self.portfolio_positions).where(
            self.portfolio_positions.c.portfolio_run_id == portfolio_run_id
        )
        return pd.read_sql_query(query, self.engine)

    def upsert_position(
        self,
        portfolio_run_id: int,
        ticker: str,
        quantity: float,
        avg_cost_cad: float,
        last_price_cad: float,
    ) -> None:
        if quantity <= 1e-9:
            with self.engine.begin() as connection:
                connection.execute(
                    self.portfolio_positions.delete().where(
                        (self.portfolio_positions.c.portfolio_run_id == portfolio_run_id)
                        & (self.portfolio_positions.c.ticker == ticker)
                    )
                )
            return

        market_value = quantity * last_price_cad
        row = {
            "portfolio_run_id": portfolio_run_id,
            "ticker": ticker,
            "quantity": quantity,
            "avg_cost_cad": avg_cost_cad,
            "last_price_cad": last_price_cad,
            "market_value_cad": market_value,
            "unrealized_pnl_cad": market_value - (quantity * avg_cost_cad),
            "updated_at_utc": datetime.now(timezone.utc),
        }

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.portfolio_positions,
                [row],
                conflict_columns=["portfolio_run_id", "ticker"],
                update_columns=[
                    "quantity",
                    "avg_cost_cad",
                    "last_price_cad",
                    "market_value_cad",
                    "unrealized_pnl_cad",
                    "updated_at_utc",
                ],
            )

    def insert_trade(
        self,
        portfolio_run_id: int,
        ticker: str,
        side: str,
        quantity: float,
        price_cad: float,
        gross_cad: float,
        reason: str,
        insight_id: int | None = None,
        fee_cad: float = 0.0,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                self.portfolio_trades.insert().values(
                    portfolio_run_id=portfolio_run_id,
                    insight_id=insight_id,
                    traded_at_utc=datetime.now(timezone.utc),
                    ticker=ticker,
                    side=side,
                    quantity=quantity,
                    price_cad=price_cad,
                    gross_cad=gross_cad,
                    fee_cad=fee_cad,
                    reason=reason,
                )
            )

    def save_snapshot(
        self,
        portfolio_run_id: int,
        snapshot_date: date,
        cash_cad: float,
        positions_value_cad: float,
        total_equity_cad: float,
        starting_cash_cad: float,
        data_source: str,
    ) -> None:
        cumulative_return_pct = (
            ((total_equity_cad - starting_cash_cad) / starting_cash_cad) * 100
            if starting_cash_cad
            else 0.0
        )
        row = {
            "portfolio_run_id": portfolio_run_id,
            "snapshot_date": snapshot_date,
            "cash_cad": cash_cad,
            "positions_value_cad": positions_value_cad,
            "total_equity_cad": total_equity_cad,
            "cumulative_return_pct": cumulative_return_pct,
            "data_source": data_source,
        }

        with self.engine.begin() as connection:
            self._upsert(
                connection,
                self.portfolio_snapshots,
                [row],
                conflict_columns=["portfolio_run_id", "snapshot_date"],
                update_columns=[
                    "cash_cad",
                    "positions_value_cad",
                    "total_equity_cad",
                    "cumulative_return_pct",
                    "data_source",
                ],
            )

    def close(self) -> None:
        self.engine.dispose()

    def _ensure_known_columns(self) -> None:
        inspector = inspect(self.engine)
        existing_tables = set(inspector.get_table_names())

        for table in (
            self.raw_headlines,
            self.scored_headlines,
            self.insights,
            self.pipeline_run_logs,
            self.insight_evaluations,
            self.price_quotes,
            self.portfolio_runs,
            self.portfolio_positions,
            self.portfolio_trades,
            self.portfolio_snapshots,
            self.health_alerts,
            self.report_runs,
        ):
            if table.name not in existing_tables:
                continue
            existing_columns = {
                column["name"]
                for column in inspector.get_columns(table.name)
            }
            missing_columns = [
                column
                for column in table.columns
                if column.name not in existing_columns and not column.primary_key
            ]
            if missing_columns:
                self._add_missing_columns(table, missing_columns)

    def _add_missing_columns(self, table: Table, columns: list[Column]) -> None:
        preparer = self.engine.dialect.identifier_preparer
        quoted_table = preparer.quote(table.name)

        with self.engine.begin() as connection:
            for column in columns:
                quoted_column = preparer.quote(column.name)
                column_type = column.type.compile(dialect=self.engine.dialect)
                connection.execute(
                    text(
                        f"ALTER TABLE {quoted_table} "
                        f"ADD COLUMN {quoted_column} {column_type} NULL"
                    )
                )

    def _raw_content_hash(self, headline: RawHeadline) -> str:
        return self._normalizer.build_content_hash(headline)

    def _scored_content_hash(self, headline: ScoredHeadline) -> str:
        if headline.content_hash:
            return headline.content_hash

        raw_headline = RawHeadline(
            ticker=headline.ticker,
            headline=headline.headline,
            source=headline.source,
            url=headline.url,
            published_at_utc=headline.published_at_utc,
            summary=headline.summary,
            category=headline.category,
            topic=headline.topic,
            industry=headline.industry,
        )
        return self._raw_content_hash(raw_headline)

    def _insert_ignore(
        self,
        connection,
        table: Table,
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
    ) -> None:
        dialect_name = connection.dialect.name

        if dialect_name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(table).values(rows)
            first_conflict_column = conflict_columns[0]
            connection.execute(
                stmt.on_duplicate_key_update(
                    {first_conflict_column: stmt.inserted[first_conflict_column]}
                )
            )
            return

        if dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(table).values(rows)
            connection.execute(
                stmt.on_conflict_do_nothing(index_elements=conflict_columns)
            )
            return

        for row in rows:
            try:
                connection.execute(table.insert().values(row))
            except IntegrityError:
                continue

    def _upsert(
        self,
        connection,
        table: Table,
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: Iterable[str],
    ) -> None:
        dialect_name = connection.dialect.name
        update_column_list = list(update_columns)

        if dialect_name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(table).values(rows)
            connection.execute(
                stmt.on_duplicate_key_update(
                    {
                        column_name: stmt.inserted[column_name]
                        for column_name in update_column_list
                    }
                )
            )
            return

        if dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(table).values(rows)
            connection.execute(
                stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_={
                        column_name: stmt.excluded[column_name]
                        for column_name in update_column_list
                    },
                )
            )
            return

        for row in rows:
            try:
                connection.execute(table.insert().values(row))
            except IntegrityError:
                criteria = [
                    table.c[column_name] == row[column_name]
                    for column_name in conflict_columns
                ]
                connection.execute(
                    table.update()
                    .where(*criteria)
                    .values({column_name: row[column_name] for column_name in update_column_list})
                )
