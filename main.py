from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
TOPIC_NAME = "financial_transactions"

logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)


def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    try:
        metadata = admin_client.list_topics(timeout=5)
        if topic_name not in metadata.topics:
            new_topic = NewTopic(
                topic=topic_name, 
                num_partitions=NUM_PARTITIONS, 
                replication_factor=REPLICATION_FACTOR
                )
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic {topic} created successfully.")
                except Exception as e:
                    logger.error(f"Error creating topic {topic}: {e}")
        else:
            logger.info(f"Topic {topic_name} already exists.")
    except Exception as e:
        logger.error(f"Error creating topic {topic_name}: {e}")

if __name__ == "__main__":
    create_topic(TOPIC_NAME)

    