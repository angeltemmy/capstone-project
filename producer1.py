from kafka import KafkaProducer
import json
import time
import datetime
from faker import Faker
from confluent_kafka import Producer, Consumer



def read_config(config_file):
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open(config_file) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split("=", 1)
        config[parameter] = value.strip()
  return config

# Required for correctness in Apache Kafka clients prior to 2.6
#client.dns.lookup=use_all_dns_ips

# Best practice for higher availability in Apache Kafka clients prior to 3.0
#session.timeout.ms=45000










topic = "random"
#BOOTSTRAP_SERVER = 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'

#'pkc-619z3.us-east1.gcp.confluent.cloud:9092'
#'localhost:9092'

fake = Faker()

def get_random_invoice():
    tax_percentage = 12
    net_price =   
    date_time_invoice_created = fake.date_time_between(start_date='-20y', end_date='now', tzinfo=None) \
        .strftime('%Y-%m-%dT%H:%M:%S')
    return {
        "id": fake.random_number(digits=12),
        "name": fake.name(),
        "date": date_time_invoice_created,
        "address": fake.address().replace("\n", " "),
        "startGate": fake.city(),
        "exitGate": fake.city(),
        "price": {
            "net": net_price,
            "taxPercentage": tax_percentage,
            "total": net_price + (net_price * (tax_percentage / 100))
        }
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = Producer(read_config("client.properties"))
   #bootstrap_servers=[BOOTSTRAP_SERVER],api_version=(0,11,5),value_serializer=json_serializer)

if __name__ == "__main__":
    while 1:
        random_invoice = get_random_invoice()
        print("{}: {}".format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'), random_invoice))
        producer.produce(topic, value=json_serializer(random_invoice))
        producer.flush()
        time.sleep(fake.random_int(0, 3))

# Run the producer.py script: python producer.py





def produce(topic, config):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  key = "key"
  value = "value"
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()

def consume(topic, config):
  # sets the consumer group ID and offset
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)

  # subscribes to the specified topic
  consumer.subscribe([topic])

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

def main():
  config = read_config()
  topic = "random"

  produce(topic, config)
  consume(topic, config)


main()
