
from confluent_kafka import Producer
import json
import time
from faker import Faker
import sys
sys.path.append('path_to_common_module_directory')
from config_reader import read_config

fake = Faker()

def generate_bank_details(payment_id):
    return {
        "BankDetailID": fake.random_int(1, 50000),
        "Payments_PaymentID": payment_id,
        "BankName": fake.company(),
        "AccountNumber": fake.random_int(10000000, 99999999),
        "IBAN": fake.random_int(1000000000, 9999999999),
        "BIC": fake.random_int(100000, 999999)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_bank_details_data(topic, config):
    producer = Producer(config)
    print("Starting Bank Details Producer...")

    try:
        while True:
            payment_id = fake.random_int(1, 3000)
            bank_detail = generate_bank_details(payment_id)
            producer.produce(topic, key="bank_detail", value=json_serializer(bank_detail))
            print(f"Sent Bank Detail: {bank_detail}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Bank Details Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "bank_details"
    produce_bank_details_data(topic, config)
