from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from analytics.insight_engine import InsightEngine
from config import settings
from config.watchlist import filter_to_sp500_tickers, get_default_watchlist
from ingestion.finnhub_client import FinnhubClient
from models.raw_headline import RawHeadline
from simulation.insight_evaluator import InsightPerformanceEvaluator
from simulation.mock_exchange import MockExchange
from simulation.price_provider import build_price_provider
from sentiment.finbert_scorer import FinBERTScorer
from storage.factory import build_storage
from streaming.news_producer import NewsProducer
from transformations.normalize_headlines import normalize_headlines
from transformations.headline_normalizer import HeadlineNormalizer

REQUIRED_SNOWFLAKE_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)

PLACEHOLDER_MARKERS = (
    "replace_with",
    "replace-with",
    "your_",
    "changeme",
    "change_me",
)


@dataclass(slots=True)
class BackfillStats:
    requests_attempted: int = 0
    headlines_fetched: int = 0
    headlines_after_dedupe: int = 0
    raw_saved: int = 0
    kafka_published: int = 0
    scored_saved: int = 0
    failed_requests: int = 0
    insights_generated: int = 0
    backtest_days: int = 0
    evaluations_saved: int = 0


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Expected date in YYYY-MM-DD format, got: {value}"
        ) from error


def parse_ticker_list(value: str) -> list[str]:
    return [
        ticker.strip().upper()
        for ticker in value.split(",")
        if ticker.strip()
    ]


def load_tickers_from_file(path: Path) -> list[str]:
    tickers: list[str] = []

    for line in path.read_text().splitlines():
        ticker = line.strip()
        if ticker and not ticker.startswith("#"):
            tickers.append(ticker.upper())

    return tickers


def unique_preserving_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    unique_values: list[str] = []

    for value in values:
        normalized = value.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_values.append(normalized)

    return unique_values


def resolve_tickers(args: argparse.Namespace) -> list[str]:
    if args.large_cap_50:
        tickers = get_default_watchlist()
    elif args.ticker_file:
        tickers = load_tickers_from_file(args.ticker_file)
    elif args.tickers:
        tickers = parse_ticker_list(args.tickers)
    else:
        tickers = settings.default_tickers

    return filter_to_sp500_tickers(unique_preserving_order(tickers))


def date_windows(
    start_date: date,
    end_date: date,
    chunk_days: int,
) -> Iterable[tuple[date, date]]:
    current_start = start_date

    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=chunk_days - 1),
            end_date,
        )
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)


def is_missing_or_placeholder(value: str | None) -> bool:
    if not value:
        return True

    normalized_value = value.strip().lower()
    return any(marker in normalized_value for marker in PLACEHOLDER_MARKERS)


def validate_environment(args: argparse.Namespace) -> None:
    if args.plan_only:
        return

    if is_missing_or_placeholder(settings.finnhub_api_key):
        raise ValueError(
            "FINNHUB_API_KEY is required for historical backfill and cannot "
            "be a placeholder value."
        )

    if args.dry_run or args.storage_backend != "snowflake":
        return

    missing_snowflake_vars = [
        name
        for name in REQUIRED_SNOWFLAKE_ENV_VARS
        if is_missing_or_placeholder(os.getenv(name))
    ]

    if missing_snowflake_vars:
        raise ValueError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing_snowflake_vars)
        )


def dedupe_headlines(headlines: list[RawHeadline]) -> list[RawHeadline]:
    normalizer = HeadlineNormalizer()
    seen_hashes: set[str] = set()
    unique_headlines: list[RawHeadline] = []

    for headline in headlines:
        content_hash = normalizer.build_content_hash(headline)
        if content_hash in seen_hashes:
            continue

        seen_hashes.add(content_hash)
        unique_headlines.append(headline)

    return unique_headlines


def attach_content_hashes(scored_headlines) -> None:
    normalizer = HeadlineNormalizer()

    for scored_headline in scored_headlines:
        raw_headline = RawHeadline(
            ticker=scored_headline.ticker,
            headline=scored_headline.headline,
            source=scored_headline.source,
            url=scored_headline.url,
            published_at_utc=scored_headline.published_at_utc,
            summary=scored_headline.summary,
            category=scored_headline.category,
            topic=scored_headline.topic,
            industry=scored_headline.industry,
        )
        scored_headline.content_hash = normalizer.build_content_hash(raw_headline)


def fetch_with_retries(
    client: FinnhubClient,
    ticker: str,
    from_date: date,
    to_date: date,
    retry_attempts: int,
    retry_sleep_seconds: float,
) -> list[RawHeadline]:
    last_error: Exception | None = None

    for attempt in range(1, retry_attempts + 1):
        try:
            return client.fetch_company_news(
                ticker=ticker,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
            )
        except Exception as error:
            last_error = error
            logging.warning(
                "Fetch failed for %s %s..%s on attempt %s/%s: %s",
                ticker,
                from_date,
                to_date,
                attempt,
                retry_attempts,
                error,
            )
            if attempt < retry_attempts:
                time.sleep(retry_sleep_seconds)

    raise RuntimeError(
        f"Failed to fetch {ticker} {from_date}..{to_date}"
    ) from last_error


def fetch_adaptive_windows(
    client: FinnhubClient,
    ticker: str,
    from_date: date,
    to_date: date,
    retry_attempts: int,
    retry_sleep_seconds: float,
    split_threshold: int,
    stats: BackfillStats,
) -> list[tuple[date, date, list[RawHeadline]]]:
    stats.requests_attempted += 1

    try:
        headlines = fetch_with_retries(
            client=client,
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            retry_attempts=retry_attempts,
            retry_sleep_seconds=retry_sleep_seconds,
        )
    except Exception:
        stats.failed_requests += 1
        logging.exception(
            "Skipping failed window for %s %s..%s.",
            ticker,
            from_date,
            to_date,
        )
        return []

    window_days = (to_date - from_date).days + 1
    if split_threshold and len(headlines) >= split_threshold and window_days > 1:
        midpoint = from_date + timedelta(days=(window_days // 2) - 1)
        next_start = midpoint + timedelta(days=1)
        logging.info(
            "%s %s..%s fetched=%s, splitting window to avoid API caps.",
            ticker,
            from_date,
            to_date,
            len(headlines),
        )
        return (
            fetch_adaptive_windows(
                client=client,
                ticker=ticker,
                from_date=from_date,
                to_date=midpoint,
                retry_attempts=retry_attempts,
                retry_sleep_seconds=retry_sleep_seconds,
                split_threshold=split_threshold,
                stats=stats,
            )
            + fetch_adaptive_windows(
                client=client,
                ticker=ticker,
                from_date=next_start,
                to_date=to_date,
                retry_attempts=retry_attempts,
                retry_sleep_seconds=retry_sleep_seconds,
                split_threshold=split_threshold,
                stats=stats,
            )
        )

    return [(from_date, to_date, headlines)]


def build_parser() -> argparse.ArgumentParser:
    today = date.today()
    default_from_date = today - timedelta(days=183)

    parser = argparse.ArgumentParser(
        description=(
            "Backfill historical Finnhub headlines into configured storage, with optional "
            "Kafka publishing and FinBERT scoring."
        )
    )
    parser.add_argument("--from-date", type=parse_date, default=default_from_date)
    parser.add_argument("--to-date", type=parse_date, default=today)
    parser.add_argument("--chunk-days", type=int, default=7)
    parser.add_argument("--tickers")
    parser.add_argument("--ticker-file", type=Path)
    parser.add_argument(
        "--large-cap-50",
        action="store_true",
        help="Compatibility alias; selects the static S&P 500 universe.",
    )
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    parser.add_argument("--retry-attempts", type=int, default=3)
    parser.add_argument("--retry-sleep-seconds", type=float, default=3.0)
    parser.add_argument("--split-threshold", type=int, default=240)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--create-tables", action="store_true")
    parser.add_argument(
        "--storage-backend",
        choices=["mysql", "local", "local_mysql", "snowflake"],
        default=settings.storage_backend,
    )
    parser.add_argument("--publish-kafka", action="store_true")
    parser.add_argument("--score-and-save", action="store_true")
    parser.add_argument(
        "--skip-backtest",
        action="store_true",
        help="Skip the simple historical recommendation/backtest step.",
    )
    parser.add_argument("--log-level", default=settings.log_level)
    return parser


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.chunk_days < 1:
        raise ValueError("--chunk-days must be at least 1.")
    if args.from_date > args.to_date:
        raise ValueError("--from-date must be earlier than or equal to --to-date.")

    tickers = resolve_tickers(args)
    validate_environment(args)

    logging.info(
        "Starting historical backfill for %s ticker(s), %s through %s.",
        len(tickers),
        args.from_date,
        args.to_date,
    )

    planned_requests = len(tickers) * len(
        list(date_windows(args.from_date, args.to_date, args.chunk_days))
    )
    logging.info(
        "Backfill plan: planned_requests=%s, chunk_days=%s, tickers=%s",
        planned_requests,
        args.chunk_days,
        ",".join(tickers),
    )

    if args.plan_only:
        return

    if len(tickers) < 1:
        logging.warning(
            "No selected ticker is in the static S&P 500 universe."
        )

    client = FinnhubClient(api_key=settings.finnhub_api_key)
    storage = None if args.dry_run else build_storage(args.storage_backend)
    producer = None
    scorer = None
    stats = BackfillStats()

    try:
        if storage and args.create_tables:
            storage.create_tables()

        if args.publish_kafka and not args.dry_run:
            producer = NewsProducer(
                kafka_broker=settings.kafka_broker,
                topic=settings.raw_headlines_topic,
            )

        if args.score_and_save and not args.dry_run:
            scorer = FinBERTScorer(model_name=settings.finbert_model_name)

        stop_requested = False

        for ticker in tickers:
            if stop_requested:
                break

            for window_start, window_end in date_windows(
                args.from_date,
                args.to_date,
                args.chunk_days,
            ):
                if args.max_requests and stats.requests_attempted >= args.max_requests:
                    stop_requested = True
                    break

                window_results = fetch_adaptive_windows(
                    client=client,
                    ticker=ticker,
                    from_date=window_start,
                    to_date=window_end,
                    retry_attempts=args.retry_attempts,
                    retry_sleep_seconds=args.retry_sleep_seconds,
                    split_threshold=args.split_threshold,
                    stats=stats,
                )

                for result_start, result_end, raw_headlines in window_results:
                    normalized_headlines = normalize_headlines(raw_headlines)
                    unique_headlines = dedupe_headlines(normalized_headlines)

                    stats.headlines_fetched += len(raw_headlines)
                    stats.headlines_after_dedupe += len(unique_headlines)

                    if not args.dry_run and storage:
                        storage.save_raw_headlines(unique_headlines)
                        stats.raw_saved += len(unique_headlines)

                    if producer:
                        producer.publish_batch(unique_headlines)
                        stats.kafka_published += len(unique_headlines)

                    if scorer and storage:
                        scored_headlines = scorer.score_batch(unique_headlines)
                        attach_content_hashes(scored_headlines)
                        storage.save_scored_headlines(scored_headlines)
                        stats.scored_saved += len(scored_headlines)

                    logging.info(
                        "%s %s..%s fetched=%s unique=%s",
                        ticker,
                        result_start,
                        result_end,
                        len(raw_headlines),
                        len(unique_headlines),
                    )

                    if args.sleep_seconds:
                        time.sleep(args.sleep_seconds)

        if (
            storage
            and args.score_and_save
            and not args.skip_backtest
            and is_local_storage_backend(args.storage_backend)
        ):
            backtest_summary = run_historical_recommendation_backtest(
                storage=storage,
                tickers=tickers,
                from_date=args.from_date,
                to_date=args.to_date,
            )
            stats.insights_generated = int(backtest_summary["insights_generated"])
            stats.backtest_days = int(backtest_summary["backtest_days"])
            stats.evaluations_saved = int(backtest_summary["evaluations_saved"])
            logging.info("Historical backtest summary: %s", backtest_summary)

    finally:
        if storage:
            storage.close()

    logging.info(
        "Backfill complete: requests=%s failed_requests=%s fetched=%s "
        "unique=%s raw_saved=%s kafka_published=%s scored_saved=%s "
        "insights_generated=%s backtest_days=%s evaluations_saved=%s",
        stats.requests_attempted,
        stats.failed_requests,
        stats.headlines_fetched,
        stats.headlines_after_dedupe,
        stats.raw_saved,
        stats.kafka_published,
        stats.scored_saved,
        stats.insights_generated,
        stats.backtest_days,
        stats.evaluations_saved,
    )


def is_local_storage_backend(storage_backend: str) -> bool:
    return storage_backend.lower() in {"mysql", "local", "local_mysql"}


def run_historical_recommendation_backtest(
    storage,
    tickers: list[str],
    from_date: date,
    to_date: date,
    run_name: str = "six-month-backfill",
) -> dict[str, object]:
    scored = storage.fetch_dashboard_table("scored_headlines")
    if scored.empty:
        return {
            "insights_generated": 0,
            "backtest_days": 0,
            "evaluations_saved": 0,
            "final_equity_cad": None,
        }

    ticker_set = set(tickers)
    scored = scored.copy()
    scored["ticker"] = scored["ticker"].astype(str).str.upper()
    scored["published_at_utc"] = pd.to_datetime(
        scored["published_at_utc"],
        errors="coerce",
        utc=True,
    )
    start_timestamp = pd.Timestamp(from_date, tz="UTC")
    end_timestamp = pd.Timestamp(to_date + timedelta(days=1), tz="UTC")
    scored = scored[
        scored["ticker"].isin(ticker_set)
        & (scored["published_at_utc"] >= start_timestamp)
        & (scored["published_at_utc"] < end_timestamp)
    ]
    if scored.empty:
        return {
            "insights_generated": 0,
            "backtest_days": 0,
            "evaluations_saved": 0,
            "final_equity_cad": None,
        }

    price_provider = build_price_provider(storage=storage)
    insight_engine = InsightEngine()
    mock_exchange = MockExchange(storage, price_provider=price_provider)
    evaluation = None
    last_result = None
    insights_generated = 0
    backtest_days = 0

    for run_date in sorted(scored["published_at_utc"].dt.date.dropna().unique()):
        window_start = pd.Timestamp(
            run_date - timedelta(days=settings.lookback_days),
            tz="UTC",
        )
        window_end = pd.Timestamp(run_date + timedelta(days=1), tz="UTC")
        window = scored[
            (scored["published_at_utc"] >= window_start)
            & (scored["published_at_utc"] < window_end)
        ]
        insights = insight_engine.generate_insights(window, as_of_date=run_date)
        if not insights:
            continue

        storage.save_insights(insights)
        insights_generated += len(insights)
        latest_insights = storage.fetch_latest_insights(as_of_date=run_date)
        last_result = mock_exchange.rebalance_from_insights(
            insights=latest_insights,
            as_of_date=run_date,
            run_name=run_name,
            starting_cash_cad=settings.portfolio_initial_cash_cad,
            max_positions=settings.portfolio_max_positions,
            cash_reserve_pct=settings.portfolio_cash_reserve_pct,
        )
        backtest_days += 1

    if backtest_days:
        evaluation = InsightPerformanceEvaluator(
            storage,
            price_provider=price_provider,
        ).evaluate_all(as_of_date=to_date)

    return {
        "insights_generated": insights_generated,
        "backtest_days": backtest_days,
        "evaluations_saved": evaluation.evaluations_saved if evaluation else 0,
        "final_equity_cad": last_result.total_equity_cad if last_result else None,
    }


if __name__ == "__main__":
    main()
