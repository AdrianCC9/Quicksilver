from dotenv import load_dotenv
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from sentiment.scorer_factory import build_sentiment_scorer
from storage.factory import build_storage
from streaming.sentiment_consumer import SentimentConsumer

def main() -> None:
    """
    Read raw headlines from Kafka, score them with FinBERT,
    and save the scored headline records into configured storage.
    """

    # Load local/Snowflake credentials and other config values from .env.
    load_dotenv()

    scorer = build_sentiment_scorer()
    storage = build_storage()

    consumer = SentimentConsumer(
        kafka_broker=settings.kafka_broker,
        topic=settings.raw_headlines_topic,
        group_id=settings.sentiment_consumer_group_id,
        scorer=scorer,
    )

    try:
        if hasattr(storage, "create_tables"):
            storage.create_tables()

        # Consumer reads RawHeadline objects from Kafka and returns ScoredHeadline objects.
        scored_headlines = consumer.consume(max_messages=settings.sentiment_max_messages)

        # Save scored headlines into the configured storage backend.
        storage.save_scored_headlines(scored_headlines)

        print(f"Saved {len(scored_headlines)} scored headlines")

    finally:
        storage.close()

if __name__ == "__main__":
    main()
