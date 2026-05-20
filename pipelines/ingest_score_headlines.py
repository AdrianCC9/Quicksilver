from dotenv import load_dotenv

from config import settings
from sentiment.finbert_scorer import FinBERTScorer
from storage.snowflake_storage import SnowflakeStorage
from streaming.sentiment_consumer import SentimentConsumer

def main() -> None:
    """
    Read raw headlines from Kafka, score them with FinBERT,
    and save the scored headline records into Snowflake.
    """

    # Load Snowflake credentials and other config values from .env.
    load_dotenv()

    scorer = FinBERTScorer()
    storage = SnowflakeStorage()

    consumer = SentimentConsumer(
        kafka_broker=settings.kafka_broker,
        topic=settings.raw_headlines_topic,
        group_id=settings.sentiment_consumer_group_id,
        scorer=scorer,
    )

    try:
        # Consumer reads RawHeadline objects from Kafka and returns ScoredHeadline objects.
        scored_headlines = consumer.consume(max_messages=100)

        # Save scored headlines into Snowflake.
        storage.save_scored_headlines(scored_headlines)

        print(f"Saved {len(scored_headlines)} scored headlines")

    finally:
        storage.close()

if __name__ == "__main__":
    main()
