from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
import logging
from typing import Dict
import threading
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
HEARTBEAT_TIMEOUT = 10  # Timeout in seconds to consider a node DOWN
MONITOR_INTERVAL = 5    # Interval to check heartbeats

class KafkaToElasticsearch:
    def __init__(self):
        # List of Kafka topics to consume
        self.topics = ['info_logs', 'error_logs', 'warn_logs', 'registration_logs', 'heartbeat_logs']
        
        # Kafka Configuration
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=['192.168.83.128:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='python-es-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Elasticsearch Configuration
        self.es = Elasticsearch(
            ['https://localhost:9200'],
            basic_auth=('elastic', '2-H9bgWSw+ujt-96NyqB'),
            verify_certs=False
        )

        # Tracking the last heartbeat timestamps for nodes
        self.node_status = {}  # {node_id: {"service_name": str, "last_heartbeat": datetime, "status": str}}

    def get_index_name(self, topic: str) -> str:
        """Generate index name based on topic and current date."""
        prefix = topic.replace('_logs', 'kafka')
        return f"{prefix}-{datetime.now().strftime('%Y.%m.%d')}"

    def update_node_status(self, node_id: str, service_name: str, status: str, timestamp: str):
        """Update or index the registration log with the new status."""
        try:
            index_name = "kafka-registration"
            doc_id = f"{node_id}-{service_name}"
            doc = {
                "message_type": "REGISTRATION",
                "node_id": node_id,
                "service_name": service_name,
                "status": status,
                "timestamp": timestamp,
                "@timestamp": datetime.now().isoformat()  # Add @timestamp for Kibana
            }
            # Index or update the document in Elasticsearch
            response = self.es.index(index=index_name, id=doc_id, document=doc)
            logger.info(f"Updated status for node {node_id}: {response['result']}")
        except Exception as e:
            logger.error(f"Error updating status for node {node_id}: {str(e)}")

    def process_message(self, topic: str, log_entry: Dict) -> None:
        """Process a single message."""
        try:
            # Add timestamp if not present
            if '@timestamp' not in log_entry:
                log_entry['@timestamp'] = datetime.now().isoformat()

            # Process registration logs
            if topic == 'registration_logs':
                node_id = log_entry['node_id']
                service_name = log_entry['service_name']
                timestamp = log_entry['timestamp']
                # Mark node as "UP" and store registration data
                self.node_status[node_id] = {
                    "service_name": service_name,
                    "last_heartbeat": datetime.now(),
                    "status": "UP"
                }
                self.update_node_status(node_id, service_name, "UP", timestamp)

            # Process heartbeat logs
            elif topic == 'heartbeat_logs':
                node_id = log_entry['node_id']
                if node_id in self.node_status:
                    self.node_status[node_id]["last_heartbeat"] = datetime.now()

            # Log other message types to their respective indices
            else:
                index_name = self.get_index_name(topic)
                self.es.index(index=index_name, document=log_entry)

        except Exception as e:
            logger.error(f"Error processing message from {topic}: {str(e)}")
            logger.error(f"Message content: {log_entry}")

    def process_messages(self):
        """Main processing loop for consuming and indexing messages."""
        try:
            logger.info(f"Starting to consume messages from topics: {', '.join(self.topics)}")
            for message in self.consumer:
                try:
                    topic = message.topic
                    log_entry = message.value
                    self.process_message(topic, log_entry)
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Critical error: {str(e)}")
        finally:
            self.cleanup()

    def monitor_heartbeats(self):
        """Monitor heartbeats and update node statuses."""
        while True:
            now = datetime.now()
            for node_id, data in list(self.node_status.items()):
                if now - data["last_heartbeat"] > timedelta(seconds=HEARTBEAT_TIMEOUT) and data["status"] == "UP":
                    logger.warning(f"Node {node_id} has stopped sending heartbeats.")
                    self.update_node_status(node_id, data["service_name"], "DOWN", now.isoformat())
                    self.node_status[node_id]["status"] = "DOWN"
            time.sleep(MONITOR_INTERVAL)

    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        try:
            self.consumer.close()
            self.es.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def check_elasticsearch_connection(es: Elasticsearch) -> bool:
    """Check if Elasticsearch is available."""
    try:
        return es.ping()
    except Exception as e:
        logger.error(f"Elasticsearch connection error: {str(e)}")
        return False

if __name__ == "__main__":
    # First, make sure Elasticsearch is available
    es_client = Elasticsearch(
        ['https://localhost:9200'],
        basic_auth=('elastic', '2-H9bgWSw+ujt-96NyqB'),
        verify_certs=False
    )
    try:
        if not check_elasticsearch_connection(es_client):
            raise Exception("Could not connect to Elasticsearch")
        
        logger.info("Successfully connected to Elasticsearch")
        kafka_to_es = KafkaToElasticsearch()

        # Start the heartbeat monitoring thread
        heartbeat_thread = threading.Thread(target=kafka_to_es.monitor_heartbeats, daemon=True)
        heartbeat_thread.start()

        # Start processing Kafka messages
        kafka_to_es.process_messages()
    except Exception as e:
        logger.error(f"Failed to start: {str(e)}")
    finally:
        es_client.close()
