from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"SUCCESS: message delivered to {msg.topic()} partition [{msg.partition()}]")

producer = Producer({"bootstrap.servers": "localhost:9092"})

producer.produce(
    topic="raw_headlines",
    value=b"test message - hello from quicksilver",
    key=b"AAPL",
    on_delivery=delivery_callback,
)

producer.flush()
print("Done.")
