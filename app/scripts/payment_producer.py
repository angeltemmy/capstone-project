
import sys
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config
from datetime import datetime, timedelta

fake = Faker()
def generate_random_date_in_last_10_years():
    today = datetime.today()
    start_date = today - timedelta(days=365 * 10)  # 10 years ago
    random_date = start_date + timedelta(days=random.randint(0, 365 * 10))
    return random_date.isoformat()

def generate_payment(invoice_id):

    return {
        "PaymentID": fake.unique.random_int(1, 30000000),
        "Invoices_InvoiceID": invoice_id,
        "PaymentAmount": round(random.uniform(100, 5000), 2),
        "PaymentDate": generate_random_date_in_last_10_years(),
        "PaymentReference": fake.uuid4(),
        "PaymentStatus_StatusID": random.randint(1, 3),
        "PaymentMethods_MethodID": random.randint(1, 5)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_payment_data(topic, config):
    producer = Producer(config)
    print("Starting Payment Producer...")

    try:
        while True:
            invoice_id = fake.random_int(1, 2000)  # Assuming invoice IDs are pre-generated
            payment = generate_payment(invoice_id)
            producer.produce(topic, key="payment", value=json_serializer(payment))
            print(f"Sent Payment: {payment}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Payment Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "payments"
    produce_payment_data(topic, config)
