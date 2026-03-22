import time
import json
import uuid
from faker import Faker
from confluent_kafka import Producer

from loguru import logger

fake = Faker()

producer_config = {"bootstrap.servers": "kafka:9092"}

producer = Producer(producer_config)

TOPIC_NAME = "transaction"


def deliver_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {msg}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}")


def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "amount": round(fake.random.uniform(1.0, 5000.0), 2),
        "currentcy": "USD",
        "timestamp": time.time(),
        "merchant": fake.company(),
    }


def serialize_message(msg):
    return json.dumps(msg).encode("utf-8")


def run():
    logger.info("Starting transaction ingestion...")
    while True:
        transaction = generate_transaction()

        producer.produce(
            topic=TOPIC_NAME,
            key=str(transaction["user_id"]),
            value=serialize_message(transaction),
            callback=deliver_report,
        )

        producer.poll(0)

        time.sleep(3)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Stopping ingestion...")
    finally:
        producer.flush()
