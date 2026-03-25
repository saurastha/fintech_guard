import time
import uuid
import random
from faker import Faker

fake = Faker()


def generate_transaction() -> dict:
    """Generates a synthetic transaction."""
    data = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "amount": round(fake.random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "timestamp": time.time(),
        "merchant": fake.company(),
    }

    if random.random() < 0.05:
        bad_data_type = random.choice(["missing_amount", "missing_id", "invalid_type"])

        if bad_data_type == "missing_amount":
            data["amount"] = None
        elif bad_data_type == "missing_id":
            data["transaction_id"] = None
        elif bad_data_type == "invalid_type":
            data["user_id"] = "999"

    return data
