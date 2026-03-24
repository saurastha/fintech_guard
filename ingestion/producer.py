import time
import json
import random

import os
import uuid
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from loguru import logger

fake = Faker()

producer_config = {"bootstrap.servers": "kafka:9092"}

producer = Producer(producer_config)

TOPIC_NAME = "transaction"
DLQ_TOPIC_NAME = "transaction-dlq"


def deliver_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {msg}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_transaction():
    prob = random.randint(0, 10)

    data = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "amount": round(fake.random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "timestamp": time.time(),
        "merchant": fake.company(),
    }

    if prob > 9:
        data["amount"] = None
    elif prob > 9:
        data["transaction_id"] = None

    return data


def run():
    schema_file_name = "transaction.avsc"
    path = os.path.realpath(os.path.dirname(__file__))

    with open(f"{path}/{schema_file_name}") as f:
        schema_str = f.read()

    schema_registry_conf = {"url": "http://schema_registry:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(
        schema_registry_client, schema_str, conf={"auto.register.schemas": True}
    )

    logger.info("Starting transaction ingestion...")
    while True:
        transaction = generate_transaction()

        try:
            serialized_value = avro_serializer(
                transaction, SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )

            producer.produce(
                topic=TOPIC_NAME,
                key=str(transaction["user_id"]),
                value=serialized_value,
                callback=deliver_report,
            )

        except Exception as e:
            logger.warning(f"Invalid data detected! Routing to DLQ. Error: {e}")

            raw_json_bytes = json.dumps(transaction).encode("utf-8")

            producer.produce(
                topic=DLQ_TOPIC_NAME,
                key=str(transaction.get("user_id", "unknown")),
                value=raw_json_bytes,
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
