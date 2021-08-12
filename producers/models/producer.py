"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

ZOOKEEPER_SERVER_URL = 'http://128.199.56.236:2181'
SCHEMA_REGISTRY_URL = 'http://128.199.56.236:8081'
KAFKA_SERVER_URL = 'PLAINTEXT://128.199.56.236:9092'


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "bootstrap.servers": KAFKA_SERVER_URL
        }
        self.client = AdminClient(self.broker_properties)

        self.schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties,
                                     schema_registry=self.schema_registry
                                     )

    def topic_exists(self):
        return self.client.list_topics(timeout=5).topics.get(self.topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if not self.topic_exists():
            self.client.create_topics([NewTopic(self.topic_name, self.num_partitions, self.num_replicas)])
            logger.info(f"topic {self.topic_name} created")
            return
        logger.info("topic already exists - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.poll(0)
        self.producer.flush()

        logger.info("producer closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
