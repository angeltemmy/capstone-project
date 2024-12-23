
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
    status_mapping = {
        1: "Pending",
        2: "Completed",
        3: "Failed",
        4: "Processing"
    }
    status_id = random.choice(list(status_mapping.keys()))
    status_name = status_mapping[status_id]
    return {
        "StatusID": status_id,
        "StatusName": status_name,
        "Description": fake.text(max_nb_chars=30)
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
