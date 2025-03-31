import json
import threading
import uuid
import random
import time
from confluent_kafka import Producer
import logging
from queue import Queue
from typing import Dict, List
import signal
import sys

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
TOPIC_NAME = "financial_transactions"
BATCH_SIZE = 1000
NUM_THREADS = 16  # Tăng số lượng threads
QUEUE_SIZE = 100000  # Queue size cho message buffer

# Cấu hình producer tối ưu
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 2097151,  # ~2GB
    "batch.size": 1000000,  # 1MB batch size
    "linger.ms": 5,  # Đợi 5ms để gom batch
    "compression.type": "lz4",  # LZ4 compression nhanh hơn snappy
    "acks": "1",
    "max.in.flight.requests.per.connection": 5,
    "socket.send.buffer.bytes": 1048576,  # 1MB socket buffer
    "socket.receive.buffer.bytes": 1048576
}

# Message queue để buffer messages
message_queue = Queue(maxsize=QUEUE_SIZE)
running = True

def signal_handler(signum, frame):
    global running
    logger.info("Shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def generate_transaction() -> Dict:
    """Generate một transaction với cache sẵn các giá trị random"""
    merchant_ids = ["merchant_" + str(i) for i in range(1, 4)]
    payment_methods = ["credit_card", "paypal", "bank_transfer"]
    currencies = ['USD', 'VND', 'EUR']
    locations = [f'location_{i}' for i in range(1, 51)]
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100)}",
        "amount": round(random.uniform(50000, 150000), 2),
        "transaction_time": int(time.time()),
        "merchant_id": random.choice(merchant_ids),
        "transaction_type": "purchase" if random.random() > 0.5 else "refund",
        "location": random.choice(locations),
        "payment_method": random.choice(payment_methods),
        "is_international": random.random() > 0.5,
        "currency": random.choice(currencies)
    }

def delivery_callback(err, msg):
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def producer_worker():
    """Worker thread để gửi messages từ queue"""
    producer = Producer(producer_conf)
    
    while running:
        try:
            messages = []
            # Gom nhiều messages thành batch
            for _ in range(BATCH_SIZE):
                if not running:
                    break
                try:
                    msg = message_queue.get(timeout=0.1)
                    messages.append(msg)
                except Queue.Empty:
                    break

            # Gửi batch messages
            for msg in messages:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=msg['user_id'],
                    value=json.dumps(msg).encode('utf-8'),
                    callback=delivery_callback
                )
            
            producer.poll(0)  # Trigger delivery reports
            
        except Exception as e:
            logger.error(f"Producer error: {e}")
    
    # Flush khi shutdown
    producer.flush(timeout=5)

def generator_worker():
    """Worker thread để generate transactions"""
    while running:
        try:
            transaction = generate_transaction()
            message_queue.put(transaction, timeout=0.1)
        except Queue.Full:
            time.sleep(0.001)  # Ngủ ngắn nếu queue đầy
        except Exception as e:
            logger.error(f"Generator error: {e}")

if __name__ == "__main__":
    try:
        # Start generator threads
        generator_threads = [
            threading.Thread(target=generator_worker, daemon=True)
            for _ in range(NUM_THREADS // 2)
        ]
        
        # Start producer threads
        producer_threads = [
            threading.Thread(target=producer_worker, daemon=True)
            for _ in range(NUM_THREADS // 2)
        ]
        
        # Start all threads
        for t in generator_threads + producer_threads:
            t.start()
            
        # Wait for threads
        while running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        running = False
        
    finally:
        # Wait for threads to finish
        for t in generator_threads + producer_threads:
            t.join(timeout=5)
        logger.info("Shutdown complete")



    