import json
import logging
import threading
from typing import Callable, Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from giggityflix_tracker.config import config
from giggityflix_tracker.models import (
    CatalogAvailabilityChanged, CatalogSubscriptionMatched,
    PeerCatalogUpdated, PeerConnected, PeerDisconnected
)

logger = logging.getLogger(__name__)


class KafkaService:
    def __init__(self):
        self.kafka_config = config.kafka
        self.producer = Producer({
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'client.id': f'{self.kafka_config.group_id}-producer'
        })
        self.consumer = None
        self.running = False
        self.consumer_thread = None

    # === Producer methods ===

    def publish_catalog_availability_changed(self, event: CatalogAvailabilityChanged) -> None:
        """Publish a catalog.availability.changed event."""
        # Convert the model to dict with custom datetime handling
        event_dict = self._model_to_dict(event)
        self._publish_message(
            self.kafka_config.catalog_availability_changed_topic,
            event_dict
        )

    def publish_catalog_subscription_matched(self, event: CatalogSubscriptionMatched) -> None:
        """Publish a catalog.subscription.matched event."""
        # Convert the model to dict with custom datetime handling
        event_dict = self._model_to_dict(event)
        # Convert PeerInfo objects to dicts
        event_dict["available_peers"] = [self._model_to_dict(peer) for peer in event.available_peers]
        
        self._publish_message(
            self.kafka_config.catalog_subscription_matched_topic,
            event_dict
        )

    def _model_to_dict(self, model) -> Dict[str, Any]:
        """Convert a model to dict with custom datetime handling."""
        result = {}
        for key, value in model.model_dump().items():
            # Handle datetime serialization
            if hasattr(value, 'isoformat'):
                result[key] = value.isoformat()
            elif isinstance(value, list) and value and hasattr(value[0], 'model_dump'):
                # Handle lists of models
                result[key] = [self._model_to_dict(item) for item in value]
            else:
                result[key] = value
        return result

    def _publish_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Publish a message to a Kafka topic."""
        try:
            # Convert message to JSON
            message_json = json.dumps(message)

            # Publish message
            self.producer.produce(
                topic,
                value=message_json.encode('utf-8'),
                callback=self._delivery_report
            )

            # Flush to ensure message is sent
            self.producer.flush()
            
            logger.debug(f"Published message to {topic}")

        except Exception as e:
            logger.error(f"Failed to publish message to {topic}: {e}")

    def _delivery_report(self, err, msg) -> None:
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    # === Consumer methods ===

    def start_consuming(self,
                        peer_connected_handler: Callable[[PeerConnected], None],
                        peer_disconnected_handler: Callable[[PeerDisconnected], None],
                        peer_catalog_updated_handler: Callable[[PeerCatalogUpdated], None]) -> None:
        """Start consuming Kafka messages."""
        if self.running:
            return

        self.running = True

        # Create consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'group.id': self.kafka_config.group_id,
            'auto.offset.reset': 'earliest'
        })

        # Subscribe to topics
        self.consumer.subscribe([
            self.kafka_config.peer_connected_topic,
            self.kafka_config.peer_disconnected_topic,
            self.kafka_config.peer_catalog_updated_topic
        ])

        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_loop,
            args=(peer_connected_handler, peer_disconnected_handler, peer_catalog_updated_handler),
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info(f"Kafka consumer started, listening to topics: "
                   f"{self.kafka_config.peer_connected_topic}, "
                   f"{self.kafka_config.peer_disconnected_topic}, "
                   f"{self.kafka_config.peer_catalog_updated_topic}")

    def _consume_loop(self,
                      peer_connected_handler: Callable[[PeerConnected], None],
                      peer_disconnected_handler: Callable[[PeerDisconnected], None],
                      peer_catalog_updated_handler: Callable[[PeerCatalogUpdated], None]) -> None:
        """Main consumer loop."""
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        continue
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                        continue

                # Process message
                try:
                    # Parse message
                    message_json = msg.value().decode('utf-8')
                    message = json.loads(message_json)

                    # Convert ISO format dates to datetime objects
                    self._convert_iso_dates(message)

                    # Handle message based on topic
                    if msg.topic() == self.kafka_config.peer_connected_topic:
                        event = PeerConnected(**message)
                        peer_connected_handler(event)
                        logger.debug(f"Processed peer connected event: {event.peer_id}")

                    elif msg.topic() == self.kafka_config.peer_disconnected_topic:
                        event = PeerDisconnected(**message)
                        peer_disconnected_handler(event)
                        logger.debug(f"Processed peer disconnected event: {event.peer_id}")

                    elif msg.topic() == self.kafka_config.peer_catalog_updated_topic:
                        event = PeerCatalogUpdated(**message)
                        peer_catalog_updated_handler(event)
                        logger.debug(f"Processed peer catalog updated event: {event.peer_id} with {len(event.catalog_ids)} catalog IDs")

                except Exception as e:
                    logger.error(f"Failed to process message: {e}")

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")

    def _convert_iso_dates(self, data: Dict[str, Any]) -> None:
        """Convert ISO format date strings to datetime objects."""
        from datetime import datetime
        for key, value in data.items():
            if isinstance(value, str) and 'T' in value:
                try:
                    data[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass
            elif isinstance(value, dict):
                self._convert_iso_dates(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._convert_iso_dates(item)

    def stop_consuming(self) -> None:
        """Stop consuming Kafka messages."""
        if not self.running:
            return
            
        logger.info("Stopping Kafka consumer...")
        self.running = False
        
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5.0)
            if self.consumer_thread.is_alive():
                logger.warning("Kafka consumer thread did not terminate gracefully")
        
        logger.info("Kafka consumer stopped")
