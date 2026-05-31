"""
Opt-in integration test for the Kafka -> FinBERT path.

Requires:
  - Kafka running with `docker compose up -d`
  - RUN_KAFKA_INTEGRATION=true
  - FinBERT model availability
"""

import os
from datetime import datetime, timezone

import pytest

from models.raw_headline import RawHeadline
from sentiment.finbert_scorer import FinBERTScorer
from streaming.news_producer import NewsProducer
from streaming.sentiment_consumer import SentimentConsumer


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_KAFKA_INTEGRATION", "false").lower() != "true",
    reason="Kafka integration tests are opt-in. Set RUN_KAFKA_INTEGRATION=true.",
)


def test_kafka_to_finbert_pipeline_smoke():
    broker = "localhost:9092"
    topic = "test_pipeline"
    group = "test-group"

    test_headline = RawHeadline(
        ticker="AAPL",
        headline="Apple reports record quarterly revenue beating analyst expectations",
        source="Reuters",
        url="https://example.com/apple-earnings",
        published_at_utc=datetime.now(timezone.utc),
        summary=None,
    )

    producer = NewsProducer(kafka_broker=broker, topic=topic)
    producer.publish_batch([test_headline])

    scorer = FinBERTScorer()
    consumer = SentimentConsumer(
        kafka_broker=broker,
        topic=topic,
        group_id=group,
        scorer=scorer,
    )
    results = consumer.consume(max_messages=5)

    assert len(results) >= 1
    assert results[0].ticker == "AAPL"
    assert results[0].sentiment_label in ("positive", "negative", "neutral")
