import json
import logging
from datetime import datetime
from typing import Any

from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline
from sentiment.finbert_scorer import FinBERTScorer

try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    Consumer = None  # type: ignore[assignment]
    KafkaError = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


class SentimentConsumer:
    def __init__(self, kafka_broker: str, topic: str, group_id: str, scorer: FinBERTScorer):
        if Consumer is None:
            raise RuntimeError(
                "Kafka consumption requires the optional confluent-kafka dependency. "
                "Install requirements/full.txt to use the streaming pipeline."
            )

        self._topic = topic
        self._scorer = scorer

        self._consumer: Any = Consumer({
            "bootstrap.servers": kafka_broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        })

    def _deserialize(self, raw_bytes: bytes) -> RawHeadline:
        data = json.loads(raw_bytes.decode("utf-8"))

        return RawHeadline(
            ticker=data["ticker"],
            headline=data["headline"],
            source=data["source"],
            url=data["url"],
            published_at_utc=datetime.fromisoformat(data["published_at_utc"]),
            summary=data.get("summary"),
        )

    def consume(self, max_messages: int = 10) -> list[ScoredHeadline]:
        self._consumer.subscribe([self._topic])

        headlines = []
        empty_polls = 0

        try:
            while len(headlines) < max_messages:
                msg = self._consumer.poll(timeout=5.0)

                if msg is None:
                    empty_polls += 1
                    if empty_polls >= 3:
                        logger.info("No messages waiting, stopping.")
                        break
                    continue

                if msg.error():
                    if (
                        KafkaError is not None
                        and msg.error().code() == KafkaError._PARTITION_EOF
                    ):
                        logger.info("Reached end of topic.")
                        break
                    raise RuntimeError(f"Consumer error: {msg.error()}")

                try:
                    headline = self._deserialize(msg.value())
                except Exception as error:
                    logger.warning("Skipping malformed Kafka message: %s", error)
                    continue

                headlines.append(headline)
                logger.info("Consumed: %s - %s", headline.ticker, headline.headline[:60])

        finally:
            self._consumer.close()

        scored_headlines = self._scorer.score_batch(headlines)
        return scored_headlines
