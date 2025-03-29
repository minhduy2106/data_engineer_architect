import json
import threading
import uuid
import random
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

KAFKA_BROKER = "localhost:29092"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"

logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "queue.buffering.max.messages": 10000,
    "queue.buffering.max.kbytes": 512000,
    "batch.num.messages": 10000,
    "linger.ms": 10,
    "acks": 1,
    "compression.type": "snappy"
}

producer = Producer(**producer_conf)

def generate_transaction():
    return dict(
        transaction_id=str(uuid.uuid4()),
        user_id=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transaction_time=int(time.time()),
        merchant_id=random.choice(["merchant_1", "merchant_2","merchant_3"]),
        transaction_type=random.choice(["purchase","refund"]),
        location=f'location_{random.randint(1, 50)}',
        payment_method=random.choice(["credit_card","paypal","bank_transfer"]),
        is_international=random.choice([True, False]),
        currency=random.choice(['USD','VND','EUR'])
    )

def delivery_report(err, mess):
    if err is not None:
        print(f"Delivery report error: {mess.key()}")
    else:
        print(f"Delivery report success: {mess.key()}")

def producer_transaction(thread_id):
    while True:
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['user_id'],
                value=json.dumps(transaction).encode("utf-8"),
                on_delivery=delivery_report
            )
            print(f"Thread {thread_id} - Produced transaction: {transaction}")
            producer.flush()
        except Exception as e:
            print(f"Error: {e}")


def producer_data_in_parallel(num_thread):
    threads = []
    try:
        for i in range(num_thread):
            thread = threading.Thread(target=producer_transaction, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    producer_data_in_parallel(5)



    