import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "transaction")
DLQ_TOPIC_NAME = os.environ.get("DLQ_TOPIC_NAME", "transaction-dlq")
