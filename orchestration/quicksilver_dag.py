import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

PROJECT_ROOT = Path(
    os.getenv("QUICKSILVER_PROJECT_ROOT")
    or Path(__file__).resolve().parents[1]
).resolve()

load_dotenv(PROJECT_ROOT / ".env", override=False)

DBT_PROJECT_DIR = Path(
    os.getenv("DBT_PROJECT_DIR")
    or PROJECT_ROOT / "dbt"
).expanduser().resolve()
DBT_PROFILES_DIR = Path(
    os.getenv("DBT_PROFILES_DIR")
    or Path.home() / ".dbt"
).expanduser().resolve()
DBT_TARGET = os.getenv("DBT_TARGET")

# Add project root to import search path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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

PLACEHOLDER_MARKERS = (
    "replace_with",
    "replace-with",
    "your_",
    "changeme",
    "change_me",
)


def _load_runtime_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    logging.getLogger().setLevel(settings.log_level.upper())


def _is_missing_or_placeholder(value: str | None) -> bool:
    if not value:
        return True

    normalized_value = value.strip().lower()
    return any(marker in normalized_value for marker in PLACEHOLDER_MARKERS)


def _validate_path(path: Path, description: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {description}: {path}")


def _find_dbt_executable() -> str:
    dbt_path = shutil.which("dbt")
    if dbt_path:
        return dbt_path

    local_dbt_path = PROJECT_ROOT / ".venv" / "bin" / "dbt"
    if local_dbt_path.exists():
        return str(local_dbt_path)

    raise FileNotFoundError(
        "Could not find dbt. Install it with: python -m pip install dbt-snowflake"
    )


def _dbt_command(*args: str) -> list[str]:
    command = [
        _find_dbt_executable(),
        *args,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ]

    if DBT_TARGET:
        command.extend(["--target", DBT_TARGET])

    return command


def _run_dbt_command(*args: str) -> None:
    command = _dbt_command(*args)

    logging.info("Running dbt command: %s", " ".join(command))
    completed_process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=False,
    )

    if completed_process.stdout:
        logging.info("dbt stdout:\n%s", completed_process.stdout)
    if completed_process.stderr:
        logging.warning("dbt stderr:\n%s", completed_process.stderr)

    if completed_process.returncode != 0:
        raise RuntimeError(
            "dbt command failed with exit code "
            f"{completed_process.returncode}: {' '.join(command)}"
        )


@dag(
    dag_id="quicksilver_headline_sentiment",
    description="Ingest Finnhub headlines, stream through Kafka, score with FinBERT, load Snowflake, and run dbt models.",
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
    tags=["quicksilver", "finnhub", "kafka", "finbert", "snowflake", "dbt"],
)
def quicksilver_headline_sentiment():
    @task
    def validate_runtime_config() -> None:
        _load_runtime_environment()

        missing = [
            name
            for name in REQUIRED_ENV_VARS
            if _is_missing_or_placeholder(os.getenv(name))
        ]
        if missing:
            raise ValueError(
                "Missing required environment variables or placeholder values: "
                + ", ".join(sorted(missing))
            )

        _validate_path(DBT_PROJECT_DIR / "dbt_project.yml", "dbt project file")
        _validate_path(DBT_PROFILES_DIR / "profiles.yml", "dbt profiles file")

        logging.info(
            "Quicksilver config loaded: tickers=%s, lookback_days=%s, "
            "polling_interval_minutes=%s, kafka_broker=%s, raw_topic=%s, "
            "scored_topic=%s, consumer_group=%s, finbert_model=%s, "
            "dbt_project_dir=%s, dbt_profiles_dir=%s, dbt_target=%s",
            settings.default_tickers,
            settings.lookback_days,
            settings.polling_interval_minutes,
            settings.kafka_broker,
            settings.raw_headlines_topic,
            settings.scored_headlines_topic,
            settings.sentiment_consumer_group_id,
            settings.finbert_model_name,
            DBT_PROJECT_DIR,
            DBT_PROFILES_DIR,
            DBT_TARGET or "default",
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

    @task
    def run_dbt_models() -> None:
        _load_runtime_environment()

        if (DBT_PROJECT_DIR / "packages.yml").exists():
            _run_dbt_command("deps")

        _run_dbt_command("run")

    config_ok = validate_runtime_config()
    tables_ready = ensure_snowflake_tables()
    raw_loaded = ingest_raw_headlines()
    scored_loaded = score_headlines()
    dbt_models_ready = run_dbt_models()

    config_ok >> tables_ready >> raw_loaded >> scored_loaded >> dbt_models_ready

quicksilver_headline_sentiment_dag = quicksilver_headline_sentiment()
