import os

import pytest
from confluent_kafka import Producer


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_KAFKA_INTEGRATION", "false").lower() != "true",
    reason="Kafka integration tests are opt-in. Set RUN_KAFKA_INTEGRATION=true.",
)


def test_kafka_producer_delivery_smoke():
    delivered = []
    failed = []

    def delivery_callback(err, msg):
        if err:
            failed.append(err)
        else:
            delivered.append((msg.topic(), msg.partition()))

    producer = Producer({"bootstrap.servers": "localhost:9092"})

    producer.produce(
        topic="raw_headlines",
        value=b"test message - hello from quicksilver",
        key=b"AAPL",
        on_delivery=delivery_callback,
    )

    producer.flush(timeout=10)

    assert not failed
    assert delivered
