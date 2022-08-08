"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
# SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"
# BROKER_URL = "PLAINTEXT://kafka0:9092"

class Producer:
    """Defines and provides common functionality amongst Producers"""

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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #

        self.broker_properties = {
            # TODO
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # Tracks existing topic
        existing_topic = self.topic_exists()

        # If the topic does not already exist, try to create it
        if existing_topic is False:
            self.create_topic()
     
        # TODO: Configure the AvroProducer
        self.schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.producer = AvroProducer(self.broker_properties, 
                                     default_key_schema=key_schema, 
                                     default_value_schema=value_schema)

    def topic_exists(self):
        """Checks if the given topic exists"""
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        topic_metadata = client.list_topics(timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                logger.info("topic creation kafka integration incomplete - skipping")
                raise
    
    """Duplicated function"""
    # def time_millis(self):
    #     return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        try:
            self.producer.flush()
            self.producer.close()
        except Exception as e:
            print(f"failed to close: {e}")
            logger.info("producer close incomplete - skipping")        

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
