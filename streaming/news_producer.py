import json
from confluent_kafka import Producer
from models.raw_headline import RawHeadline

class NewsProducer:
    def __init__(self, kafka_broker: str, topic: str):
        self._topic = topic
        self._producer = Producer({"bootstrap.servers": kafka_broker})

    def _serialize(self, headline: RawHeadline) -> bytes:
        # Convert raw headline data into bytes
        data = {
            "ticker": headline.ticker,
            "headline": headline.headline,
            "source": headline.source,
            "url": headline.url,
            "published_at_utc": headline.published_at_utc.isoformat(),
            "summary": headline.summary,
        }
        return json.dumps(data).encode("utf-8")
    
    def publish_headline(self, headline: RawHeadline) -> None:
        # Send one headline to the topic
        self._producer.produce(
            topic=self._topic,
            value=self._serialize(headline),
            key=headline.ticker.encode("utf-8"),
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def publish_batch(self, headlines: list[RawHeadline]) -> None:
        # Send a whole list of headlines and wait for confirmation
        for headline in headlines:
            self.publish_headline(headline)
        self._producer.flush()
    
    @staticmethod
    def _delivery_callback(err, msg):
        # Kafka calls this when message is confirmed delivered
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}]")
    
