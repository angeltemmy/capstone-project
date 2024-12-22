
from confluent_kafka import Producer
import json
import time
from faker import Faker
import sys
sys.path.insert(0, '/path/to/common/module')
from config_reader import read_config

fake = Faker()

def generate_payment_logs(payment_id):
    fake.unique.clear()
    return {
        "LogID": fake.random_int(1, 50000),
        "Timestamp": fake.date_time_this_year().isoformat(),
        "LogMessage": fake.sentence(nb_words=8),
        "Payments_PaymentID": payment_id
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_payment_logs_data(topic, config):
    producer = Producer(config)
    print("Starting Payment Logs Producer...")

    try:
        while True:
            payment_id = fake.random_int(1, 3000)
            payment_log = generate_payment_logs(payment_id)
            producer.produce(topic, key="payment_log", value=json_serializer(payment_log))
            print(f"Sent Payment Log: {payment_log}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Payment Logs Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "payment_logs"
    produce_payment_logs_data(topic, config)
