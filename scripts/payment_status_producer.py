
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'common')))
from config_reader import read_config

fake = Faker()

def generate_payment_status():
    fake.unique.clear()
    return {
        "StatusID": fake.random_int(1, 5),
        "StatusName": random.choice(["Pending", "Completed", "Failed", "Processing"]),
        "Description": fake.text(max_nb_chars=100)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_payment_status_data(topic, config):
    producer = Producer(config)
    print("Starting Payment Status Producer...")

    try:
        while True:
            payment_status = generate_payment_status()
            producer.produce(topic, key="payment_status", value=json_serializer(payment_status))
            print(f"Sent Payment Status: {payment_status}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Payment Status Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "payment_status"
    produce_payment_status_data(topic, config)
