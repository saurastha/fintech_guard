import os
import sys
import time
import json
import signal
from loguru import logger
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_NAME,
    DLQ_TOPIC_NAME,
)
from generator import generate_transaction

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def deliver_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def shutdown_handler(sig, frame):
    """Handles Docker sending SIGTERM or user sending SIGINT (Ctrl+C)"""
    logger.info("Received shutdown signal. Flushing producer and exiting gracefully...")
    producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def run():
    schema_file_name = "transaction.avsc"
    path = os.path.realpath(os.path.dirname(__file__))

    with open(os.path.join(path, schema_file_name)) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client, schema_str, conf={"auto.register.schemas": True}
    )

    logger.info(
        f"Starting transaction ingestion... Connected to {KAFKA_BOOTSTRAP_SERVERS}"
    )

    while True:
        transaction = generate_transaction()
        user_id = str(transaction.get("user_id", "unknown"))

        try:
            serialized_value = avro_serializer(
                transaction, SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )

            producer.produce(
                topic=TOPIC_NAME,
                key=user_id,
                value=serialized_value,
                callback=deliver_report,
            )

        except Exception as e:
            logger.warning(f"Invalid data detected! Routing to DLQ. Error: {e}")

            try:
                raw_json_bytes = json.dumps(transaction).encode("utf-8")
                producer.produce(
                    topic=DLQ_TOPIC_NAME,
                    key=user_id,
                    value=raw_json_bytes,
                    callback=deliver_report,
                )
            except Exception as dlq_e:
                logger.critical(
                    f"CRITICAL: Failed to route to DLQ! Data lost. Error: {dlq_e}"
                )

        producer.poll(0)
        time.sleep(3)


if __name__ == "__main__":
    run()
