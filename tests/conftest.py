import json
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from redis import Redis

# Mock protobuf modules until we generate them
sys.modules['src.grpc.generated.tracker_pb2'] = MagicMock()
sys.modules['src.grpc.generated.tracker_pb2_grpc'] = MagicMock()

from src.models import (
    CatalogAvailabilityChanged, CatalogSubscription, CatalogSubscriptionMatched,
    PeerCatalogUpdated, PeerConnected, PeerConnectionState, PeerDisconnected,
    PeerInfo, SubscriptionStatus
)
from src.services.kafka_service import KafkaService
from src.services.redis_service import RedisService
from src.services.subscription_service import SubscriptionService
from src.services.tracker_service import TrackerService


# ====== Test Data Fixtures ======

@pytest.fixture
def test_data():
    """Central fixture providing test data constants used across tests."""
    now = datetime.now()
    return {
        "peer_id": "test-peer-123",
        "edge_id": "test-edge-456",
        "catalog_id": "test-catalog-789",
        "subscription_id": "test-subscription-012",
        "service_id": "test-service-345",
        "request_id": "test-request-678",
        "timestamp": now,
        "expiration": now + timedelta(hours=24),
    }


@pytest.fixture
def sample_peer_info(test_data):
    """Create a sample PeerInfo for testing."""
    return PeerInfo(
        peer_id=test_data["peer_id"],
        edge_id=test_data["edge_id"],
        state=PeerConnectionState.CONNECTED,
        connected_at=test_data["timestamp"],
        last_heartbeat=test_data["timestamp"],
    )


@pytest.fixture
def sample_peer_connected(test_data):
    """Create a sample PeerConnected event for testing."""
    return PeerConnected(
        peer_id=test_data["peer_id"],
        edge_id=test_data["edge_id"],
        catalog_ids=[test_data["catalog_id"], "another-catalog-id"],
        timestamp=test_data["timestamp"],
    )


@pytest.fixture
def sample_peer_disconnected(test_data):
    """Create a sample PeerDisconnected event for testing."""
    return PeerDisconnected(
        peer_id=test_data["peer_id"],
        edge_id=test_data["edge_id"],
        timestamp=test_data["timestamp"],
    )


@pytest.fixture
def sample_peer_catalog_updated(test_data):
    """Create a sample PeerCatalogUpdated event for testing."""
    return PeerCatalogUpdated(
        peer_id=test_data["peer_id"],
        catalog_ids=[test_data["catalog_id"], "another-catalog-id"],
        is_full_update=True,
        timestamp=test_data["timestamp"],
    )


@pytest.fixture
def sample_catalog_subscription(test_data):
    """Create a sample CatalogSubscription for testing."""
    return CatalogSubscription(
        subscription_id=test_data["subscription_id"],
        service_id=test_data["service_id"],
        catalog_id=test_data["catalog_id"],
        created_at=test_data["timestamp"],
        expires_at=test_data["expiration"],
        status=SubscriptionStatus.ACTIVE,
    )


@pytest.fixture
def sample_catalog_availability_changed(test_data):
    """Create a sample CatalogAvailabilityChanged event for testing."""
    return CatalogAvailabilityChanged(
        catalog_id=test_data["catalog_id"],
        available_peer_count=1,
        is_available=True,
        timestamp=test_data["timestamp"],
    )


@pytest.fixture
def sample_catalog_subscription_matched(test_data, sample_peer_info):
    """Create a sample CatalogSubscriptionMatched event for testing."""
    return CatalogSubscriptionMatched(
        subscription_id=test_data["subscription_id"],
        service_id=test_data["service_id"],
        catalog_id=test_data["catalog_id"],
        available_peers=[sample_peer_info],
        timestamp=test_data["timestamp"],
    )


# ====== Reset Mock Fixtures ======

@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mocks after each test to prevent test isolation issues."""
    yield
    # Reset all mock objects after the test


# ====== Mock Service Fixtures ======

@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    redis_mock = MagicMock(spec=Redis)

    # Set up common redis mock behaviors
    redis_mock.hget.return_value = None
    redis_mock.hgetall.return_value = {}
    redis_mock.get.return_value = None
    redis_mock.keys.return_value = []
    redis_mock.smembers.return_value = set()
    redis_mock.exists.return_value = False

    # Configure pipeline method
    pipeline_mock = MagicMock()
    pipeline_mock.__enter__.return_value = pipeline_mock
    pipeline_mock.__exit__.return_value = None
    pipeline_mock.hmset = MagicMock()
    pipeline_mock.hset = MagicMock()
    pipeline_mock.expire = MagicMock()
    pipeline_mock.sadd = MagicMock()
    pipeline_mock.srem = MagicMock()
    pipeline_mock.delete = MagicMock()
    pipeline_mock.execute = MagicMock(return_value=[])
    redis_mock.pipeline.return_value = pipeline_mock

    # For Redis 4.x compatibility (hmset is deprecated)
    redis_mock.hmset = MagicMock()
    redis_mock.hset = MagicMock()
    redis_mock.sadd = MagicMock()
    redis_mock.srem = MagicMock()
    redis_mock.delete = MagicMock()
    redis_mock.expire = MagicMock()
    redis_mock.execute = MagicMock(return_value=[])

    return redis_mock


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer for testing."""
    producer_mock = MagicMock()
    producer_mock.produce.return_value = None
    producer_mock.flush.return_value = None
    return producer_mock


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka Consumer for testing."""
    consumer_mock = MagicMock()

    # Mock the poll method to return None (no message)
    consumer_mock.poll.return_value = None

    # Mock other methods
    consumer_mock.subscribe.return_value = None
    consumer_mock.close.return_value = None

    return consumer_mock


# ====== Service Fixtures ======

@pytest.fixture
def redis_service(mock_redis):
    """Create a RedisService with mocked Redis client."""
    with patch('src.services.redis_service.logger'):  # Mock logger to prevent logging in tests
        return RedisService(mock_redis)


@pytest.fixture
def kafka_service(mock_kafka_producer, mock_kafka_consumer):
    """Create a KafkaService with mocked Producer and Consumer."""
    # Patch the Producer constructor
    with patch("src.services.kafka_service.Producer", return_value=mock_kafka_producer), \
            patch('src.services.kafka_service.logger'):  # Mock logger
        # Create service and inject mocked producer
        service = KafkaService()
        service.producer = mock_kafka_producer
        # Explicitly set consumer to None and running to False for test isolation
        service.consumer = None
        service.running = False
        service.consumer_thread = None
        return service


@pytest.fixture
def subscription_service(redis_service, kafka_service):
    """Create a SubscriptionService with mocked dependencies."""
    with patch('src.services.subscription_service.logger'):  # Mock logger
        return SubscriptionService(redis_service, kafka_service)


@pytest.fixture
def tracker_service(redis_service, kafka_service, subscription_service):
    """Create a TrackerService with mocked dependencies."""
    # Patch the asyncio.create_task to prevent background tasks
    with patch("asyncio.create_task"), \
            patch('src.services.tracker_service.logger'):  # Mock logger
        return TrackerService(redis_service, kafka_service, subscription_service)


# ====== Event Simulation Helpers ======

class KafkaMockMessage:
    """Mock class for Kafka messages."""

    def __init__(self, topic, value, error=None):
        self._topic = topic
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


def create_kafka_message(topic, value_dict, error=None):
    """Create a mock Kafka message with the specified topic and value."""

    # Handle datetime serialization
    def serialize(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    value_bytes = json.dumps(value_dict, default=serialize).encode("utf-8")
    return KafkaMockMessage(topic, value_bytes, error)


def create_kafka_error(error_code):
    """Create a mock Kafka error with the specified error code."""
    error = MagicMock()
    error.code.return_value = error_code
    return error


# ====== Async Test Helpers ======

@pytest.fixture
def mock_future():
    """Create a mock Future object for async tests."""
    future = MagicMock()
    future.done.return_value = True
    future.result.return_value = None
    future.exception.return_value = None
    return future


# ====== gRPC Test Helpers ======

@pytest.fixture
def mock_grpc_context():
    """Create a mock gRPC context for server tests."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


@pytest.fixture
def mock_pb2():
    """Mock for the generated tracker_pb2 module."""
    mock = MagicMock()

    # Create mocks for response classes
    mock.PeersResponse = MagicMock()
    mock.PeersResponse.return_value = MagicMock()

    mock.PeerInfo = MagicMock()
    mock.PeerInfo.return_value = MagicMock()

    mock.CatalogIdsResponse = MagicMock()
    mock.CatalogIdsResponse.return_value = MagicMock()

    mock.ForwardResponseMessage = MagicMock()
    mock.ForwardResponseMessage.return_value = MagicMock()

    mock.SubscriptionResponse = MagicMock()
    mock.SubscriptionResponse.return_value = MagicMock()

    return mock
