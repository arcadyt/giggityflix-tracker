import uuid

from tests.conftest import *  # Import fixtures


@pytest.mark.unit
class TestSubscriptionService:

    def test_create_subscription(self, subscription_service, redis_service, kafka_service, test_data):
        """Test creating a new subscription."""
        # Setup mocks
        with patch("uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = uuid.UUID(int=0)

            # Mock redis_service.create_subscription to return success
            redis_service.create_subscription = MagicMock(return_value=True)

            # Mock check_subscription_fulfillment
            subscription_service.check_subscription_fulfillment = MagicMock(return_value=False)

            # Call method
            result = subscription_service.create_subscription(
                test_data["service_id"],
                test_data["catalog_id"]
            )

            # Check result
            assert result is not None
            assert result.subscription_id == "00000000-0000-0000-0000-000000000000"
            assert result.service_id == test_data["service_id"]
            assert result.catalog_id == test_data["catalog_id"]
            assert result.status == SubscriptionStatus.ACTIVE

            # Verify redis_service.create_subscription was called
            redis_service.create_subscription.assert_called_once()

            # Verify check_subscription_fulfillment was called
            subscription_service.check_subscription_fulfillment.assert_called_once_with(result)

    def test_create_subscription_failed(self, subscription_service, redis_service, test_data):
        """Test creating a subscription that fails in Redis."""
        # Setup mocks
        redis_service.create_subscription = MagicMock(return_value=False)

        # Call method
        result = subscription_service.create_subscription(
            test_data["service_id"],
            test_data["catalog_id"]
        )

        # Check result
        assert result is None

        # Verify redis_service.create_subscription was called
        redis_service.create_subscription.assert_called_once()

    def test_check_subscription_fulfillment_not_active(self, subscription_service, sample_catalog_subscription):
        """Test checking fulfillment of a non-active subscription."""
        # Modify subscription to be non-active
        sample_catalog_subscription.status = SubscriptionStatus.FULFILLED

        # Call method
        result = subscription_service.check_subscription_fulfillment(sample_catalog_subscription)

        # Check result
        assert result is False

    def test_check_subscription_fulfillment_no_peers(self, subscription_service, redis_service,
                                                     sample_catalog_subscription):
        """Test checking fulfillment when no peers are available."""
        # Mock redis_service.get_peer_info_for_catalog to return empty list
        redis_service.get_peer_info_for_catalog = MagicMock(return_value=[])

        # Call method
        result = subscription_service.check_subscription_fulfillment(sample_catalog_subscription)

        # Check result
        assert result is False

        # Verify redis_service.get_peer_info_for_catalog was called
        redis_service.get_peer_info_for_catalog.assert_called_once_with(sample_catalog_subscription.catalog_id)

    def test_check_subscription_fulfillment_no_new_peers(self, subscription_service, redis_service,
                                                         sample_catalog_subscription, sample_peer_info):
        """Test checking fulfillment when all available peers are already notified."""
        # Add peer to already notified set
        sample_catalog_subscription.notified_peer_ids.add(sample_peer_info.peer_id)

        # Mock redis_service.get_peer_info_for_catalog to return peer
        redis_service.get_peer_info_for_catalog = MagicMock(return_value=[sample_peer_info])

        # Call method
        result = subscription_service.check_subscription_fulfillment(sample_catalog_subscription)

        # Check result
        assert result is False

        # Verify redis_service.get_peer_info_for_catalog was called
        redis_service.get_peer_info_for_catalog.assert_called_once_with(sample_catalog_subscription.catalog_id)

    def test_check_subscription_fulfillment_success(self, subscription_service, redis_service, kafka_service,
                                                    sample_catalog_subscription, sample_peer_info):
        """Test successful subscription fulfillment."""
        # Mock redis_service.get_peer_info_for_catalog to return peer
        redis_service.get_peer_info_for_catalog = MagicMock(return_value=[sample_peer_info])

        # Mock kafka_service
        kafka_service.publish_catalog_subscription_matched = MagicMock()
        
        # Mock redis_service.add_notified_peer
        redis_service.add_notified_peer = MagicMock(return_value=True)

        # Call method
        result = subscription_service.check_subscription_fulfillment(sample_catalog_subscription)

        # Check result
        assert result is True

        # Verify redis_service.get_peer_info_for_catalog was called
        assert redis_service.get_peer_info_for_catalog.call_count == 1
        assert redis_service.get_peer_info_for_catalog.call_args[0][0] == sample_catalog_subscription.catalog_id

        # Verify redis_service.add_notified_peer was called
        assert redis_service.add_notified_peer.call_count == 1
        assert redis_service.add_notified_peer.call_args[0][0] == sample_catalog_subscription.subscription_id
        assert redis_service.add_notified_peer.call_args[0][1] == sample_peer_info.peer_id

        # Verify kafka_service.publish_catalog_subscription_matched was called
        assert kafka_service.publish_catalog_subscription_matched.call_count == 1
        event = kafka_service.publish_catalog_subscription_matched.call_args[0][0]
        assert isinstance(event, CatalogSubscriptionMatched)
        assert event.subscription_id == sample_catalog_subscription.subscription_id
        assert event.service_id == sample_catalog_subscription.service_id
        assert event.catalog_id == sample_catalog_subscription.catalog_id
        assert len(event.available_peers) == 1
        assert event.available_peers[0] == sample_peer_info

    def test_check_catalog_subscriptions(self, subscription_service, redis_service, kafka_service, test_data,
                                         sample_catalog_subscription, sample_peer_info):
        """Test checking all subscriptions for a catalog ID."""
        # Mock redis_service.get_subscriptions_for_catalog to return subscription
        redis_service.get_subscriptions_for_catalog = MagicMock(return_value=[sample_catalog_subscription])

        # Mock kafka_service
        kafka_service.publish_catalog_subscription_matched = MagicMock()
        
        # Mock redis_service.add_notified_peer
        redis_service.add_notified_peer = MagicMock(return_value=True)

        # Call method
        result = subscription_service.check_catalog_subscriptions(test_data["catalog_id"], [sample_peer_info])

        # Check result
        assert result == 1  # One subscription fulfilled

        # Verify redis_service.get_subscriptions_for_catalog was called
        assert redis_service.get_subscriptions_for_catalog.call_count == 1
        assert redis_service.get_subscriptions_for_catalog.call_args[0][0] == test_data["catalog_id"]

        # Verify redis_service.add_notified_peer was called
        assert redis_service.add_notified_peer.call_count == 1
        assert redis_service.add_notified_peer.call_args[0][0] == sample_catalog_subscription.subscription_id
        assert redis_service.add_notified_peer.call_args[0][1] == sample_peer_info.peer_id

        # Verify kafka_service.publish_catalog_subscription_matched was called
        assert kafka_service.publish_catalog_subscription_matched.call_count == 1

    def test_check_catalog_subscriptions_already_notified(self, subscription_service, redis_service, test_data,
                                                          sample_catalog_subscription, sample_peer_info):
        """Test checking subscriptions when all peers are already notified."""
        # Add peer to already notified set
        sample_catalog_subscription.notified_peer_ids.add(sample_peer_info.peer_id)

        # Mock redis_service.get_subscriptions_for_catalog to return subscription
        redis_service.get_subscriptions_for_catalog = MagicMock(return_value=[sample_catalog_subscription])
        
        # Mock redis_service.add_notified_peer
        redis_service.add_notified_peer = MagicMock()
        
        # Mock kafka_service.publish_catalog_subscription_matched
        kafka_service = MagicMock()
        subscription_service.kafka_service = kafka_service

        # Call method
        result = subscription_service.check_catalog_subscriptions(test_data["catalog_id"], [sample_peer_info])

        # Check result
        assert result == 0  # No subscriptions fulfilled

        # Verify redis_service.get_subscriptions_for_catalog was called
        assert redis_service.get_subscriptions_for_catalog.call_count == 1
        assert redis_service.get_subscriptions_for_catalog.call_args[0][0] == test_data["catalog_id"]

        # Verify redis_service.add_notified_peer was not called
        assert redis_service.add_notified_peer.call_count == 0

        # Verify kafka_service.publish_catalog_subscription_matched was not called
        assert kafka_service.publish_catalog_subscription_matched.call_count == 0

    def test_get_subscription(self, subscription_service, redis_service, test_data):
        """Test getting a subscription by ID."""
        # Mock redis_service.get_subscription
        redis_service.get_subscription = MagicMock()
        
        # Call method
        subscription_service.get_subscription(test_data["subscription_id"])

        # Verify redis_service.get_subscription was called
        assert redis_service.get_subscription.call_count == 1
        assert redis_service.get_subscription.call_args[0][0] == test_data["subscription_id"]

    def test_get_subscriptions_for_service(self, subscription_service, redis_service, test_data):
        """Test getting all subscriptions for a service."""
        # Mock redis_service.get_subscriptions_for_service
        redis_service.get_subscriptions_for_service = MagicMock()
        
        # Call method
        subscription_service.get_subscriptions_for_service(test_data["service_id"])

        # Verify redis_service.get_subscriptions_for_service was called
        assert redis_service.get_subscriptions_for_service.call_count == 1
        assert redis_service.get_subscriptions_for_service.call_args[0][0] == test_data["service_id"]

    def test_cleanup_expired_subscriptions(self, subscription_service, redis_service):
        """Test cleaning up expired subscriptions."""
        # Mock redis_service.clear_expired_subscriptions to return count
        redis_service.clear_expired_subscriptions = MagicMock(return_value=5)

        # Call method
        result = subscription_service.cleanup_expired_subscriptions()

        # Check result
        assert result == 5

        # Verify redis_service.clear_expired_subscriptions was called
        assert redis_service.clear_expired_subscriptions.call_count == 1
