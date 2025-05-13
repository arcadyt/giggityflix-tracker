import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from tests.conftest import (
    create_kafka_error, create_kafka_message,
    KafkaMockMessage
)


@pytest.mark.unit
class TestKafkaService:
    def test_init(self, kafka_service, mock_kafka_producer):
        """Test KafkaService initialization."""
        # Verify producer was properly initialized
        assert kafka_service.producer == mock_kafka_producer

        # Verify other attributes
        assert kafka_service.consumer is None
        assert kafka_service.running is False
        assert kafka_service.consumer_thread is None

    def test_publish_catalog_availability_changed(self, kafka_service, sample_catalog_availability_changed):
        """Test publishing a catalog.availability.changed event."""
        # Mock _publish_message
        kafka_service._publish_message = MagicMock()

        # Call method
        kafka_service.publish_catalog_availability_changed(sample_catalog_availability_changed)

        # Verify _publish_message was called with correct arguments
        kafka_service._publish_message.assert_called_once()
        args, kwargs = kafka_service._publish_message.call_args

        # Check topic
        assert args[0] == kafka_service.kafka_config.catalog_availability_changed_topic

        # Check message
        event_dict = args[1]
        assert event_dict["catalog_id"] == sample_catalog_availability_changed.catalog_id
        assert event_dict["available_peer_count"] == sample_catalog_availability_changed.available_peer_count
        assert event_dict["is_available"] == sample_catalog_availability_changed.is_available

    def test_publish_catalog_subscription_matched(self, kafka_service, sample_catalog_subscription_matched):
        """Test publishing a catalog.subscription.matched event."""
        # Mock _publish_message
        kafka_service._publish_message = MagicMock()

        # Call method
        kafka_service.publish_catalog_subscription_matched(sample_catalog_subscription_matched)

        # Verify _publish_message was called with correct arguments
        kafka_service._publish_message.assert_called_once()
        args, kwargs = kafka_service._publish_message.call_args

        # Check topic
        assert args[0] == kafka_service.kafka_config.catalog_subscription_matched_topic

        # Check message
        event_dict = args[1]
        assert event_dict["subscription_id"] == sample_catalog_subscription_matched.subscription_id
        assert event_dict["service_id"] == sample_catalog_subscription_matched.service_id
        assert event_dict["catalog_id"] == sample_catalog_subscription_matched.catalog_id

        # Check that available_peers was converted to list of dicts
        assert isinstance(event_dict["available_peers"], list)
        assert len(event_dict["available_peers"]) == 1
        peer_dict = event_dict["available_peers"][0]
        assert isinstance(peer_dict, dict)
        assert peer_dict["peer_id"] == sample_catalog_subscription_matched.available_peers[0].peer_id

    def test_publish_message(self, kafka_service, mock_kafka_producer):
        """Test the _publish_message method."""
        # Test data
        topic = "test-topic"
        message = {"key": "value"}

        # Call method
        kafka_service._publish_message(topic, message)

        # Verify produce was called with correct arguments
        mock_kafka_producer.produce.assert_called_once()
        args, kwargs = mock_kafka_producer.produce.call_args

        # Check arguments
        assert args[0] == topic
        assert kwargs["value"] == json.dumps(message).encode('utf-8')
        assert "callback" in kwargs

        # Verify flush was called
        mock_kafka_producer.flush.assert_called_once()

    def test_delivery_report_success(self, kafka_service):
        """Test the _delivery_report callback with success."""
        # Create mock message
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0

        # Call method with no error
        with patch("src.services.kafka_service.logger") as mock_logger:
            kafka_service._delivery_report(None, msg)

            # Verify logger.debug was called
            mock_logger.debug.assert_called_once()
            assert "Message delivered" in mock_logger.debug.call_args[0][0]

    def test_delivery_report_error(self, kafka_service):
        """Test the _delivery_report callback with error."""
        # Call method with error
        with patch("src.services.kafka_service.logger") as mock_logger:
            kafka_service._delivery_report("Error message", None)

            # Verify logger.error was called
            mock_logger.error.assert_called_once()
            assert "Message delivery failed" in mock_logger.error.call_args[0][0]

    def test_start_consuming_already_running(self, kafka_service):
        """Test start_consuming when already running."""
        # Set running flag
        kafka_service.running = True

        # Create mock handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Call method
        kafka_service.start_consuming(
            peer_connected_handler,
            peer_disconnected_handler,
            peer_catalog_updated_handler
        )

        # Verify nothing happened (kafka_service.consumer is still None)
        assert kafka_service.consumer is None

    def test_start_consuming(self, kafka_service, mock_kafka_consumer):
        """Test starting the Kafka consumer."""
        # Set running flag to False
        kafka_service.running = False
        kafka_service.consumer = None

        # Create mock handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Mock thread creation
        with patch("src.services.kafka_service.Consumer", return_value=mock_kafka_consumer), \
             patch("src.services.kafka_service.threading.Thread") as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance

            # Call method
            kafka_service.start_consuming(
                peer_connected_handler,
                peer_disconnected_handler,
                peer_catalog_updated_handler
            )

            # Verify running flag
            assert kafka_service.running is True

            # Verify consumer was created
            assert kafka_service.consumer == mock_kafka_consumer

            # Verify subscription to topics
            mock_kafka_consumer.subscribe.assert_called_once()
            topics_arg = mock_kafka_consumer.subscribe.call_args[0][0]
            assert kafka_service.kafka_config.peer_connected_topic in topics_arg
            assert kafka_service.kafka_config.peer_disconnected_topic in topics_arg
            assert kafka_service.kafka_config.peer_catalog_updated_topic in topics_arg

            # Verify thread was created and started
            mock_thread.assert_called_once()
            assert mock_thread.call_args[1]["target"] == kafka_service._consume_loop
            assert mock_thread.call_args[1]["args"] == (
                peer_connected_handler,
                peer_disconnected_handler,
                peer_catalog_updated_handler
            )
            assert mock_thread.call_args[1]["daemon"] is True
            mock_thread_instance.start.assert_called_once()

    def test_stop_consuming_not_running(self, kafka_service):
        """Test stopping the consumer when not running."""
        # Set running flag to False
        kafka_service.running = False
        kafka_service.consumer_thread = None

        # Call method
        kafka_service.stop_consuming()

        # Verify nothing happened
        assert kafka_service.running is False
        assert kafka_service.consumer_thread is None

    def test_stop_consuming_running(self, kafka_service):
        """Test stopping the consumer when running."""
        # Set running flag to True and mock thread
        kafka_service.running = True
        kafka_service.consumer_thread = MagicMock()

        # Call method
        kafka_service.stop_consuming()

        # Verify running flag was set to False
        assert kafka_service.running is False

        # Verify thread was joined
        kafka_service.consumer_thread.join.assert_called_once_with(timeout=5.0)

    def test_consume_loop_peer_connected(self, kafka_service, mock_kafka_consumer, test_data, sample_peer_connected):
        """Test processing a peer.connection.established message."""
        # Set up handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message
        message = create_kafka_message(
            kafka_service.kafka_config.peer_connected_topic,
            sample_peer_connected.model_dump()
        )

        # Set running flag to True initially, but change after first message
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Setup side effect to stop after processing
        def stop_after_call(*args, **kwargs):
            kafka_service.running = False

        peer_connected_handler.side_effect = stop_after_call

        # Setup mock_kafka_consumer to return message once, then None
        mock_kafka_consumer.poll.side_effect = [message, None]

        # Call method
        kafka_service._consume_loop(
            peer_connected_handler,
            peer_disconnected_handler,
            peer_catalog_updated_handler
        )

        # Verify poll was called
        assert mock_kafka_consumer.poll.call_count > 0

        # Verify correct handler was called
        peer_connected_handler.assert_called_once()
        peer_disconnected_handler.assert_not_called()
        peer_catalog_updated_handler.assert_not_called()

        # Verify handler was called with correct event type
        event = peer_connected_handler.call_args[0][0]
        assert event.peer_id == sample_peer_connected.peer_id
        assert event.edge_id == sample_peer_connected.edge_id
        assert event.catalog_ids == sample_peer_connected.catalog_ids

    def test_consume_loop_peer_disconnected(self, kafka_service, mock_kafka_consumer, test_data, sample_peer_disconnected):
        """Test processing a peer.connection.closed message."""
        # Set up handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message
        message = create_kafka_message(
            kafka_service.kafka_config.peer_disconnected_topic,
            sample_peer_disconnected.model_dump()
        )

        # Set running flag to True initially, but change after first message
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Setup side effect to stop after processing
        def stop_after_call(*args, **kwargs):
            kafka_service.running = False

        peer_disconnected_handler.side_effect = stop_after_call

        # Setup mock_kafka_consumer to return message once, then None
        mock_kafka_consumer.poll.side_effect = [message, None]

        # Call method
        kafka_service._consume_loop(
            peer_connected_handler,
            peer_disconnected_handler,
            peer_catalog_updated_handler
        )

        # Verify poll was called
        assert mock_kafka_consumer.poll.call_count > 0

        # Verify correct handler was called
        peer_connected_handler.assert_not_called()
        peer_disconnected_handler.assert_called_once()
        peer_catalog_updated_handler.assert_not_called()

        # Verify handler was called with correct event type
        event = peer_disconnected_handler.call_args[0][0]
        assert event.peer_id == sample_peer_disconnected.peer_id
        assert event.edge_id == sample_peer_disconnected.edge_id

    def test_consume_loop_peer_catalog_updated(self, kafka_service, mock_kafka_consumer, test_data, sample_peer_catalog_updated):
        """Test processing a peer.catalog.updated message."""
        # Set up handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message
        message = create_kafka_message(
            kafka_service.kafka_config.peer_catalog_updated_topic,
            sample_peer_catalog_updated.model_dump()
        )

        # Set running flag to True initially, but change after first message
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Setup side effect to stop after processing
        def stop_after_call(*args, **kwargs):
            kafka_service.running = False

        peer_catalog_updated_handler.side_effect = stop_after_call

        # Setup mock_kafka_consumer to return message once, then None
        mock_kafka_consumer.poll.side_effect = [message, None]

        # Call method
        kafka_service._consume_loop(
            peer_connected_handler,
            peer_disconnected_handler,
            peer_catalog_updated_handler
        )

        # Verify poll was called
        assert mock_kafka_consumer.poll.call_count > 0

        # Verify correct handler was called
        peer_connected_handler.assert_not_called()
        peer_disconnected_handler.assert_not_called()
        peer_catalog_updated_handler.assert_called_once()

        # Verify handler was called with correct event type
        event = peer_catalog_updated_handler.call_args[0][0]
        assert event.peer_id == sample_peer_catalog_updated.peer_id
        assert event.catalog_ids == sample_peer_catalog_updated.catalog_ids
        assert event.is_full_update == sample_peer_catalog_updated.is_full_update

    def test_consume_loop_error_partition_eof(self, kafka_service, mock_kafka_consumer):
        """Test handling a Kafka _PARTITION_EOF error."""
        # Set up handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message with _PARTITION_EOF error
        message = KafkaMockMessage(
            "test-topic",
            None,
            create_kafka_error(KafkaError._PARTITION_EOF)
        )

        # Set running flag to True initially
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Create a list for poll results
        poll_results = [message, None]
        call_count = 0
        
        # Define a side effect function
        def poll_side_effect(*args, **kwargs):
            nonlocal call_count
            result = poll_results[call_count]
            call_count += 1
            if call_count >= len(poll_results):
                kafka_service.running = False
            return result
        
        # Set the side effect
        mock_kafka_consumer.poll = MagicMock(side_effect=poll_side_effect)

        # Call method
        kafka_service._consume_loop(
            peer_connected_handler,
            peer_disconnected_handler,
            peer_catalog_updated_handler
        )

        # Verify no handlers were called (EOF is not an error)
        peer_connected_handler.assert_not_called()
        peer_disconnected_handler.assert_not_called()
        peer_catalog_updated_handler.assert_not_called()

    def test_consume_loop_error_other(self, kafka_service, mock_kafka_consumer):
        """Test handling a non-EOF Kafka error."""
        # Set up handlers
        peer_connected_handler = MagicMock()
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message with non-PARTITION_EOF error
        message = KafkaMockMessage(
            "test-topic",
            None,
            create_kafka_error(KafkaError._ALL_BROKERS_DOWN)
        )

        # Set running flag to True initially
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Create a list for poll results
        poll_results = [message, None]
        call_count = 0
        
        # Define a side effect function
        def poll_side_effect(*args, **kwargs):
            nonlocal call_count
            result = poll_results[call_count]
            call_count += 1
            if call_count >= len(poll_results):
                kafka_service.running = False
            return result
        
        # Set the side effect
        mock_kafka_consumer.poll = MagicMock(side_effect=poll_side_effect)

        # Call method
        with patch("src.services.kafka_service.logger") as mock_logger:
            kafka_service._consume_loop(
                peer_connected_handler,
                peer_disconnected_handler,
                peer_catalog_updated_handler
            )

            # Verify logger.error was called
            mock_logger.error.assert_called_once()
            assert "Kafka consumer error" in mock_logger.error.call_args[0][0]

        # Verify no handlers were called
        peer_connected_handler.assert_not_called()
        peer_disconnected_handler.assert_not_called()
        peer_catalog_updated_handler.assert_not_called()

    def test_consume_loop_processing_error(self, kafka_service, mock_kafka_consumer, test_data, sample_peer_connected):
        """Test handling an error during message processing."""
        # Set up handlers
        peer_connected_handler = MagicMock(side_effect=Exception("Test processing error"))
        peer_disconnected_handler = MagicMock()
        peer_catalog_updated_handler = MagicMock()

        # Create mock message
        message = create_kafka_message(
            kafka_service.kafka_config.peer_connected_topic,
            sample_peer_connected.model_dump()
        )

        # Set running flag to True initially
        kafka_service.running = True
        kafka_service.consumer = mock_kafka_consumer

        # Create a list for poll results
        poll_results = [message, None]
        call_count = 0
        
        # Define a side effect function
        def poll_side_effect(*args, **kwargs):
            nonlocal call_count
            result = poll_results[call_count]
            call_count += 1
            if call_count >= len(poll_results):
                kafka_service.running = False
            return result
        
        # Set the side effect
        mock_kafka_consumer.poll = MagicMock(side_effect=poll_side_effect)

        # Call method
        with patch("src.services.kafka_service.logger") as mock_logger:
            kafka_service._consume_loop(
                peer_connected_handler,
                peer_disconnected_handler,
                peer_catalog_updated_handler
            )

            # Verify logger.error was called
            mock_logger.error.assert_called_once()
            assert "Failed to process message" in mock_logger.error.call_args[0][0]

        # Verify handler was called (and failed)
        peer_connected_handler.assert_called_once()
