from __future__ import annotations

import argparse
import logging
import sys
import time
from uuid import uuid4
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from analytics.insight_engine import InsightEngine
from alerts.local_health import (
    LocalHealthAlert,
    LocalPipelineHealthMonitor,
    send_local_health_alerts,
)
from config import settings
from config.watchlist import TOP_50_EQUITY_TICKERS, get_expanded_watchlist
from ingestion.finnhub_client import FinnhubClient
from ingestion.public_news_client import PublicNewsClient
from models.raw_headline import RawHeadline
from sentiment.scorer_factory import build_sentiment_scorer
from simulation.insight_evaluator import InsightPerformanceEvaluator
from simulation.mock_exchange import MockExchange
from simulation.price_provider import build_price_provider
from storage.local_mysql_storage import LocalMySQLStorage
from transformations.normalize_headlines import normalize_headlines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Quicksilver locally with MySQL storage, public/political news, "
            "insight generation, and the mock CAD portfolio exchange."
        )
    )
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--tickers")
    parser.add_argument("--large-cap-50", action="store_true")
    parser.add_argument("--large-cap-100", action="store_true")
    parser.add_argument("--max-tickers", type=int, default=settings.public_news_max_tickers)
    parser.add_argument("--lookback-days", type=int, default=settings.lookback_days)
    parser.add_argument("--include-finnhub", action="store_true", default=settings.finnhub_enabled)
    parser.add_argument("--skip-public-news", action="store_true")
    parser.add_argument("--skip-political-news", action="store_true")
    parser.add_argument("--sentiment-backend", default=settings.sentiment_backend)
    parser.add_argument("--skip-simulation", action="store_true")
    parser.add_argument("--skip-evaluation", action="store_true")
    parser.add_argument("--run-name", default=settings.portfolio_run_name)
    parser.add_argument("--initial-cash-cad", type=float, default=settings.portfolio_initial_cash_cad)
    parser.add_argument("--max-positions", type=int, default=settings.portfolio_max_positions)
    parser.add_argument("--cash-reserve-pct", type=float, default=settings.portfolio_cash_reserve_pct)
    parser.add_argument("--seed-demo-if-empty", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-minutes", type=float, default=settings.polling_interval_minutes)
    parser.add_argument("--log-level", default=settings.log_level)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    while True:
        run_once(args)
        if not args.loop:
            break
        sleep_seconds = max(args.interval_minutes, 1) * 60
        logging.info("Sleeping %.0f seconds before next local pipeline run.", sleep_seconds)
        time.sleep(sleep_seconds)


def run_once(args: argparse.Namespace) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex
    tickers = resolve_tickers(args)
    storage = LocalMySQLStorage(database_url=args.database_url)

    try:
        storage.create_tables()
        raw_headlines = collect_headlines(args, tickers)

        if not raw_headlines and args.seed_demo_if_empty:
            raw_headlines = build_demo_headlines(tickers[: min(len(tickers), 8)])

        normalized_headlines = normalize_headlines(raw_headlines)
        storage.save_raw_headlines(normalized_headlines)

        scorer = build_sentiment_scorer(args.sentiment_backend)
        scored_headlines = scorer.score_batch(normalized_headlines)
        storage.save_scored_headlines(scored_headlines)

        since_utc = started_at - timedelta(hours=settings.insight_lookback_hours)
        recent_scored = storage.fetch_recent_scored_headlines(since_utc, tickers=tickers)
        insights = InsightEngine().generate_insights(recent_scored, as_of_date=date.today())
        storage.save_insights(insights)

        price_provider = build_price_provider(storage=storage)
        simulation_result = None
        if not args.skip_simulation:
            latest_insights = storage.fetch_latest_insights(as_of_date=date.today())
            simulation_result = MockExchange(
                storage,
                price_provider=price_provider,
            ).rebalance_from_insights(
                insights=latest_insights,
                as_of_date=date.today(),
                run_name=args.run_name,
                starting_cash_cad=args.initial_cash_cad,
                max_positions=args.max_positions,
                cash_reserve_pct=args.cash_reserve_pct,
            )

        evaluation_summary = None
        if not args.skip_evaluation:
            evaluation_summary = InsightPerformanceEvaluator(
                storage,
                price_provider=price_provider,
            ).evaluate_all(
                as_of_date=date.today()
            )

        summary = {
            "run_id": run_id,
            "tickers": len(tickers),
            "raw_headlines_collected": len(raw_headlines),
            "raw_headlines_saved_attempted": len(normalized_headlines),
            "scored_headlines_saved_attempted": len(scored_headlines),
            "recent_scored_headlines": len(recent_scored),
            "insights_generated": len(insights),
            "simulation": asdict(simulation_result) if simulation_result else None,
            "evaluation": asdict(evaluation_summary) if evaluation_summary else None,
        }
        health_alerts = LocalPipelineHealthMonitor().evaluate_success(summary)
        summary["health_alerts"] = len(health_alerts)
        summary["health_alert_types"] = [
            alert.alert_type
            for alert in health_alerts
        ]
        storage.save_pipeline_run_log(
            run_id=run_id,
            run_type="local_pipeline",
            started_at_utc=started_at,
            finished_at_utc=datetime.now(timezone.utc),
            status="success",
            summary=summary,
        )
        persist_health_alerts(storage, health_alerts)
        print_run_summary(summary)
        return summary
    except Exception as error:
        failure_summary = {
            "run_id": run_id,
            "tickers": len(tickers),
            "raw_headlines_collected": 0,
            "scored_headlines_saved_attempted": 0,
            "insights_generated": 0,
        }
        try:
            storage.save_pipeline_run_log(
                run_id=run_id,
                run_type="local_pipeline",
                started_at_utc=started_at,
                finished_at_utc=datetime.now(timezone.utc),
                status="failed",
                summary=failure_summary,
                error_message=str(error),
            )
            persist_health_alerts(
                storage,
                LocalPipelineHealthMonitor().evaluate_failure(run_id, error),
            )
        except Exception:
            logging.exception("Failed to persist local pipeline failure log.")
        raise
    finally:
        storage.close()


def collect_headlines(args: argparse.Namespace, tickers: list[str]) -> list[RawHeadline]:
    headlines: list[RawHeadline] = []
    today = date.today()
    from_date = today - timedelta(days=args.lookback_days)

    if not args.skip_public_news and settings.public_news_enabled:
        public_client = PublicNewsClient(
            timeout_seconds=settings.public_news_timeout_seconds,
            max_items_per_feed=settings.public_news_max_items_per_feed,
        )
        headlines.extend(
            public_client.fetch_headlines(
                tickers=tickers,
                lookback_days=args.lookback_days,
                include_financial=True,
                include_political=(
                    settings.political_news_enabled and not args.skip_political_news
                ),
            )
        )

    if args.include_finnhub and settings.finnhub_api_key:
        client = FinnhubClient(api_key=settings.finnhub_api_key)
        headlines.extend(
            client.fetch_batch_news(
                tickers=tickers,
                from_date=from_date.isoformat(),
                to_date=today.isoformat(),
            )
        )
    elif args.include_finnhub:
        logging.warning("FINNHUB_API_KEY is missing, skipping Finnhub ingestion.")

    return headlines


def resolve_tickers(args: argparse.Namespace) -> list[str]:
    if args.large_cap_100:
        selected = get_expanded_watchlist()
    elif args.large_cap_50:
        selected = list(TOP_50_EQUITY_TICKERS)
    elif args.tickers:
        selected = [
            ticker.strip().upper()
            for ticker in args.tickers.split(",")
            if ticker.strip()
        ]
    else:
        selected = list(settings.default_tickers)

    unique = list(dict.fromkeys(selected))
    if args.max_tickers:
        return unique[: args.max_tickers]
    return unique


def build_demo_headlines(tickers: list[str]) -> list[RawHeadline]:
    now = datetime.now(timezone.utc)
    templates = [
        (
            "financial",
            "company_news",
            None,
            "{ticker} beats expectations as analysts upgrade growth outlook",
            "Demo Finance",
        ),
        (
            "political",
            "industry_policy",
            "policy_sensitive",
            "Policy relief and new incentives expected to benefit {ticker} industry demand",
            "Demo Policy Wire",
        ),
        (
            "financial",
            "company_news",
            None,
            "{ticker} faces lawsuit risk and warning over slowing demand",
            "Demo Finance",
        ),
    ]
    headlines: list[RawHeadline] = []

    for index, ticker in enumerate(tickers):
        category, topic, industry, template, source = templates[index % len(templates)]
        headlines.append(
            RawHeadline(
                ticker=ticker,
                headline=template.format(ticker=ticker),
                source=source,
                url=f"https://example.com/quicksilver-demo/{ticker.lower()}",
                published_at_utc=now - timedelta(minutes=index * 7),
                summary="Deterministic demo headline for local pipeline smoke tests.",
                category=category,
                topic=topic,
                industry=industry,
            )
        )

    return headlines


def persist_health_alerts(
    storage: LocalMySQLStorage,
    alerts: list[LocalHealthAlert],
) -> None:
    try:
        storage.resolve_health_alerts(LocalPipelineHealthMonitor.MONITORED_ALERT_TYPES)
        if not alerts:
            return
        storage.save_health_alerts([alert.to_row() for alert in alerts])
        send_local_health_alerts(alerts)
    except Exception:
        logging.exception("Failed to persist or send local health alerts.")


def print_run_summary(summary: dict[str, object]) -> None:
    print("Local Quicksilver pipeline complete")
    for key, value in summary.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
