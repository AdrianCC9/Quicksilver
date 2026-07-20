import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
from typing import List

from config.watchlist import get_default_watchlist

load_dotenv()


def _load_default_tickers() -> List[str]:
    configured_tickers = os.getenv("DEFAULT_TICKERS", "")
    use_custom_tickers = os.getenv("USE_CUSTOM_TICKERS", "false").lower() == "true"

    if use_custom_tickers and configured_tickers:
        return [
            ticker.strip().upper()
            for ticker in configured_tickers.split(",")
            if ticker.strip()
        ]

    additional_tickers = [
        ticker.strip().upper()
        for ticker in os.getenv("ADDITIONAL_TICKERS", "").split(",")
        if ticker.strip()
    ]
    return list(dict.fromkeys(get_default_watchlist() + additional_tickers))


@dataclass
class Settings:
    # API
    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")

    # Database / storage
    storage_backend: str = os.getenv("STORAGE_BACKEND", "mysql").lower()
    database_url: str = os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://quicksilver:quicksilver@127.0.0.1:3306/quicksilver?charset=utf8mb4",
    )

    # Kafka
    kafka_broker: str = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
    raw_headlines_topic: str = os.getenv("RAW_HEADLINES_TOPIC", "raw_headlines")
    scored_headlines_topic: str = os.getenv("SCORED_HEADLINES_TOPIC", "scored_headlines")
    sentiment_consumer_group_id: str = os.getenv(
        "SENTIMENT_CONSUMER_GROUP_ID",
        "sentiment-scorers"
    )

    # Model
    finbert_model_name: str = os.getenv(
        "FINBERT_MODEL_NAME",
        "ProsusAI/finbert"
    )
    sentiment_backend: str = os.getenv("SENTIMENT_BACKEND", "lexicon").lower()

    # Pipeline Behavior
    polling_interval_minutes: int = int(os.getenv("POLLING_INTERVAL_MINUTES", "30"))
    lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "3"))
    public_news_enabled: bool = os.getenv("PUBLIC_NEWS_ENABLED", "true").lower() == "true"
    finnhub_enabled: bool = os.getenv("FINNHUB_ENABLED", "false").lower() == "true"
    public_news_max_tickers: int = int(os.getenv("PUBLIC_NEWS_MAX_TICKERS", "100"))
    public_news_max_items_per_feed: int = int(
        os.getenv("PUBLIC_NEWS_MAX_ITEMS_PER_FEED", "12")
    )
    public_news_timeout_seconds: int = int(
        os.getenv("PUBLIC_NEWS_TIMEOUT_SECONDS", "20")
    )
    political_news_enabled: bool = (
        os.getenv("POLITICAL_NEWS_ENABLED", "true").lower() == "true"
    )
    insight_lookback_hours: int = int(os.getenv("INSIGHT_LOOKBACK_HOURS", "72"))
    insight_horizon_days: int = int(os.getenv("INSIGHT_HORIZON_DAYS", "5"))
    positive_signal_threshold: float = float(
        os.getenv("POSITIVE_SIGNAL_THRESHOLD", "0.12")
    )
    negative_signal_threshold: float = float(
        os.getenv("NEGATIVE_SIGNAL_THRESHOLD", "-0.12")
    )
    portfolio_initial_cash_cad: float = float(
        os.getenv("PORTFOLIO_INITIAL_CASH_CAD", "5000")
    )
    portfolio_run_name: str = os.getenv("PORTFOLIO_RUN_NAME", "default")
    portfolio_max_positions: int = int(os.getenv("PORTFOLIO_MAX_POSITIONS", "5"))
    portfolio_cash_reserve_pct: float = float(
        os.getenv("PORTFOLIO_CASH_RESERVE_PCT", "0.05")
    )
    usd_to_cad_rate: float = float(os.getenv("USD_TO_CAD_RATE", "1.37"))
    price_provider: str = os.getenv("PRICE_PROVIDER", "stooq").lower()
    price_provider_order: str = os.getenv(
        "PRICE_PROVIDER_ORDER",
        "polygon,alpha_vantage,yahoo,stooq",
    )
    polygon_api_key: str = os.getenv("POLYGON_API_KEY", "")
    alpha_vantage_api_key: str = os.getenv("ALPHA_VANTAGE_API_KEY", "")

    # Tickers
    default_tickers: List[str] = field(default_factory=_load_default_tickers)
    expected_daily_headline_count: int = int(
        os.getenv("EXPECTED_DAILY_HEADLINE_COUNT", "500")
    )
    health_min_raw_headlines_per_run: int = int(
        os.getenv("HEALTH_MIN_RAW_HEADLINES_PER_RUN", "50")
    )
    health_min_insights_per_run: int = int(
        os.getenv("HEALTH_MIN_INSIGHTS_PER_RUN", "10")
    )
    health_max_synthetic_evaluation_pct: float = float(
        os.getenv("HEALTH_MAX_SYNTHETIC_EVALUATION_PCT", "35")
    )
    health_stale_run_hours: float = float(os.getenv("HEALTH_STALE_RUN_HOURS", "3"))
    report_output_dir: str = os.getenv("REPORT_OUTPUT_DIR", ".data/reports")

    # Alert thresholds
    negative_sentiment_threshold: float = float(
        os.getenv("NEGATIVE_SENTIMENT_THRESHOLD", "0.75")
    )
    volume_spike_zscore_threshold: float = float(
        os.getenv("VOLUME_SPIKE_ZSCORE_THRESHOLD", "2.0")
    )

    # Alerts
    local_health_notifications_enabled: bool = (
        os.getenv("LOCAL_HEALTH_NOTIFICATIONS_ENABLED", "false").lower() == "true"
    )
    slack_enabled: bool = os.getenv("SLACK_ENABLED", "false").lower() == "true"
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")

    email_enabled: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    alert_email_to: str = os.getenv("ALERT_EMAIL_TO", "")
    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_USERNAME", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    sentiment_max_messages: int = int(os.getenv("SENTIMENT_MAX_MESSAGES", "1000"))

settings = Settings()
