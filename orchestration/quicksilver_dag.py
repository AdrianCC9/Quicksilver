import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

# Find project root in an environment variable or go up two levels from current path.
PROJECT_ROOT = Path(
    os.getenv("QUICKSILVER_PROJECT_ROOT")
    or Path(__file__).resolve().parents[1]
).resolve()

# Add project root to import search path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=False)

from config import settings


REQUIRED_ENV_VARS = {
    "FINNHUB_API_KEY",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
}


def _load_runtime_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    logging.getLogger().setLevel(settings.log_level.upper())


@dag(
    dag_id="quicksilver_headline_sentiment",
    description="Ingest Finnhub headlines, stream through kafka, score with FinBERT, and load Snowflake.",
    start_date=datetime(2026, 5, 20),
    schedule=timedelta(minutes=settings.polling_interval_minutes),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quicksilver",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["quicksilver", "finnhub", "kafka", "finbert", "snowflake"],
)
def quicksilver_headline_sentiment():
    @task
    def validate_runtime_config() -> None:
        _load_runtime_environment()

        missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
        if missing:
            raise ValueError(
                "Missing required environment variables: "
                + ", ".join(sorted(missing))
            )

        logging.info(
            "Quicksilver config loaded: tickers=%s, lookback_days=%s, "
            "polling_interval_minutes=%s, kafka_broker=%s, raw_topic=%s, "
            "scored_topic=%s, consumer_group=%s, finbert_model=%s",
            settings.default_tickers,
            settings.lookback_days,
            settings.polling_interval_minutes,
            settings.kafka_broker,
            settings.raw_headlines_topic,
            settings.scored_headlines_topic,
            settings.sentiment_consumer_group_id,
            settings.finbert_model_name,
        )

    @task
    def ensure_snowflake_tables() -> None:
        _load_runtime_environment()

        from storage.setup_snowflake import main as setup_snowflake

        setup_snowflake()

    @task
    def ingest_raw_headlines() -> None:
        _load_runtime_environment()

        from ingestion.finnhub_client import FinnhubClient
        from storage.snowflake_storage import SnowflakeStorage
        from streaming.news_producer import NewsProducer
        from transformations.normalize_headlines import normalize_headlines

        today = date.today()
        from_date = today - timedelta(days=settings.lookback_days)

        client = FinnhubClient(api_key=settings.finnhub_api_key)
        storage = SnowflakeStorage()
        producer = NewsProducer(
            kafka_broker=settings.kafka_broker,
            topic=settings.raw_headlines_topic,
        )

        try:
            raw_headlines = client.fetch_batch_news(
                tickers=settings.default_tickers,
                from_date=from_date.isoformat(),
                to_date=today.isoformat(),
            )
            normalized_headlines = normalize_headlines(raw_headlines)

            storage.save_raw_headlines(normalized_headlines)
            producer.publish_batch(normalized_headlines)

            logging.info(
                "Saved and published %s raw headlines.",
                len(normalized_headlines)
            )
        finally:
            storage.close()

    @task
    def score_headlines() -> None:
        _load_runtime_environment()

        from sentiment.finbert_scorer import FinBERTScorer
        from storage.snowflake_storage import SnowflakeStorage
        from streaming.sentiment_consumer import SentimentConsumer

        scorer = FinBERTScorer(model_name=settings.finbert_model_name)
        storage = SnowflakeStorage()
        consumer = SentimentConsumer(
            kafka_broker=settings.kafka_broker,
            topic=settings.raw_headlines_topic,
            group_id=settings.sentiment_consumer_group_id,
            scorer=scorer,
        )

        try:
            scored_headlines = consumer.consume(max_messages=100)
            storage.save_scored_headlines(scored_headlines)

            logging.info("Saved %s scored headlines.", len(scored_headlines))
        finally:
            storage.close()

    config_ok = validate_runtime_config()
    tables_ready = ensure_snowflake_tables()
    raw_loaded = ingest_raw_headlines()
    scored_loaded = score_headlines()

    config_ok >> tables_ready >> raw_loaded >> scored_loaded

quicksilver_headline_sentiment_dag = quicksilver_headline_sentiment()
