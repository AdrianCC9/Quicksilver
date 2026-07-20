from datetime import date, timedelta
from pathlib import Path
import sys

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from ingestion.finnhub_client import FinnhubClient
from storage.factory import build_storage
from streaming.news_producer import NewsProducer
from transformations.normalize_headlines import normalize_headlines

def main() -> None:
    """
    Fetch raw headlines, normalize them, save them to configured storage,
    and then publish them to Kafka.
    """
    # Load config values from .env into environment variables.
    load_dotenv()

    today = date.today()
    from_date = today - timedelta(days=settings.lookback_days)

    client = FinnhubClient()
    storage = build_storage()
    producer = NewsProducer(
        kafka_broker=settings.kafka_broker,
        topic=settings.raw_headlines_topic,
    )

    try:
        if hasattr(storage, "create_tables"):
            storage.create_tables()

        # Get raw headline objects from Finnhub
        raw_headlines = client.fetch_batch_news(
            tickers=settings.default_tickers,
            from_date=from_date.isoformat(),
            to_date=today.isoformat(),
        )

        # Normalize RawHeadline objects.
        normalized_headlines = normalize_headlines(raw_headlines)

        # Save raw data into the configured storage backend.
        storage.save_raw_headlines(normalized_headlines)

        # Send raw data into Kafka for scoring.
        producer.publish_batch(normalized_headlines)

        print(f"Saved and published {len(normalized_headlines)} raw headlines")

    finally:
        storage.close()

if __name__== "__main__":
    main()
