import os
from dataclasses import dataclass, field
from typing import List

@dataclass
class Settings:
    # API
    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")

    # Database
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///quicksilver.db")

    # Model
    finbert_model_name: str = os.getenv(
        "FINBERT_MODEL_NAME",
        "ProsusAI/finbert"
    )

    # Pipeline Behavior
    polling_interval_minutes: int = int(os.getenv("POLLING_INTERVAL_MINUTES", "30"))
    lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "3"))

    # Tickers
    default_tickers: List[str] = field(
        default_factory=lambda: os.getenv("DEFAULT_TICKERS", "AAPL,MSFT,TSLA,NVDA").split(",")
    )

    # Alert thresholds
    negative_sentiment_threshold: float = float(
        os.getenv("NEGATIVE_SENTIMENT_THRESHOLD", "0.75")
    )
    volume_spike_zscore_threshold: float = float(
        os.getenv("VOLUME_SPIKE_ZSCORE_THRESHOLD", "2.0")
    )

    # Alerts
    slack_enabled: bool = os.getenv("SLACK_ENABLED", "false").lower() == "true"
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")

    email_enabled: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: str = os.getenv("SMTP_USERNAME", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

settings = Settings()