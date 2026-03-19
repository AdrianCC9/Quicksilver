"""
Full pipeline test: NewsProducer → Kafka topic → SentimentConsumer
Verifies that a headline can travel through the pipe and come out scored.

Requires:
  - Kafka running (docker compose up -d)
  - FinBERT model downloaded (first run will download ~400MB)
"""

from datetime import datetime, timezone
from models.raw_headline import RawHeadline
from streaming.news_producer import NewsProducer
from streaming.sentiment_consumer import SentimentConsumer
from sentiment.finbert_scorer import FinBERTScorer

# --- Config ---
BROKER = "localhost:9092"
TOPIC = "test_pipeline"  # use a separate topic so we don't pollute raw_headlines
GROUP = "test-group"

# --- Create a fake headline ---
test_headline = RawHeadline(
    ticker="AAPL",
    headline="Apple reports record quarterly revenue beating analyst expectations",
    source="Reuters",
    url="https://example.com/apple-earnings",
    published_at_utc=datetime.now(timezone.utc),
    summary=None,
)

# --- Step 1: Produce ---
print("Publishing headline to Kafka...")
producer = NewsProducer(kafka_broker=BROKER, topic=TOPIC)
producer.publish_batch([test_headline])
print("Published.\n")

# --- Step 2: Consume + Score ---
print("Consuming and scoring...")
scorer = FinBERTScorer()
consumer = SentimentConsumer(kafka_broker=BROKER, topic=TOPIC, group_id=GROUP, scorer=scorer)
results = consumer.consume(max_messages=5)

# --- Step 3: Verify ---
print(f"\nGot {len(results)} scored headline(s):\n")
for sh in results:
    print(f"  Ticker:     {sh.ticker}")
    print(f"  Headline:   {sh.headline}")
    print(f"  Sentiment:  {sh.sentiment_label}")
    print(f"  Compound:   {sh.compound_score}")
    print(f"  Confidence: {sh.confidence}")
    print(f"  Source Tier: {sh.source_tier}")
    print(f"  Age (hrs):  {sh.headline_age_hours}")
    print()

assert len(results) >= 1, f"Expected 1 result, got {len(results)}"
assert results[0].ticker == "AAPL"
assert results[0].sentiment_label in ("positive", "negative", "neutral")
print("ALL CHECKS PASSED")
