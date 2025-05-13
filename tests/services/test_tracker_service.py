import asyncio

from tests.conftest import *  # Import fixtures


@pytest.mark.unit
class TestTrackerService:

    def test_init(self, tracker_service, redis_service, kafka_service, subscription_service):
        """Test TrackerService initialization."""
        # Verify services were injected
        assert tracker_service.redis_service == redis_service
        assert tracker_service.kafka_service == kafka_service
        assert tracker_service.subscription_service == subscription_service

    @pytest.mark.asyncio
    async def test_handle_peer_connected(self, tracker_service, redis_service, sample_peer_connected):
        """Test handling a peer.connection.established event."""
        # Mock redis_service.register_peer
        redis_service.register_peer = MagicMock()

        # Mock handle_peer_catalog_updated to be awaitable
        tracker_service.handle_peer_catalog_updated = AsyncMock()

        # Call method
        await tracker_service.handle_peer_connected(sample_peer_connected)

        # Verify redis_service.register_peer was called
        redis_service.register_peer.assert_called_once_with(
            sample_peer_connected.peer_id,
            sample_peer_connected.edge_id
        )

        # Verify handle_peer_catalog_updated was called
        tracker_service.handle_peer_catalog_updated.assert_called_once()
        event = tracker_service.handle_peer_catalog_updated.call_args[0][0]
        assert isinstance(event, PeerCatalogUpdated)
        assert event.peer_id == sample_peer_connected.peer_id
        assert event.catalog_ids == sample_peer_connected.catalog_ids
        assert event.is_full_update is True

    @pytest.mark.asyncio
    async def test_handle_peer_disconnected(self, tracker_service, redis_service, sample_peer_disconnected,
                                            sample_peer_info):
        """Test handling a peer.connection.closed event."""
        # Mock redis_service methods
        redis_service.get_peer_info = MagicMock(return_value=sample_peer_info)
        redis_service.disconnect_peer = MagicMock()
        redis_service.remove_peer_catalog = MagicMock(return_value={"catalog-id-1", "catalog-id-2"})

        # Mock _update_catalog_availability
        tracker_service._update_catalog_availability = AsyncMock()

        # Call method
        await tracker_service.handle_peer_disconnected(sample_peer_disconnected)

        # Verify redis_service.get_peer_info was called
        redis_service.get_peer_info.assert_called_once_with(sample_peer_disconnected.peer_id)

        # Verify redis_service.disconnect_peer was called
        redis_service.disconnect_peer.assert_called_once_with(sample_peer_disconnected.peer_id)

        # Verify redis_service.remove_peer_catalog was called
        redis_service.remove_peer_catalog.assert_called_once_with(sample_peer_disconnected.peer_id)

        # Verify _update_catalog_availability was called for each catalog ID
        assert tracker_service._update_catalog_availability.call_count == 2
        tracker_service._update_catalog_availability.assert_any_call("catalog-id-1")
        tracker_service._update_catalog_availability.assert_any_call("catalog-id-2")

    @pytest.mark.asyncio
    async def test_handle_peer_catalog_updated(self, tracker_service, redis_service, sample_peer_catalog_updated):
        """Test handling a peer.catalog.updated event."""
        # Mock redis_service.update_peer_catalog to return affected IDs
        redis_service.update_peer_catalog = MagicMock(return_value=({"new-id-1"}, {"removed-id-1"}))

        # Mock _update_catalog_availability
        tracker_service._update_catalog_availability = AsyncMock()

        # Call method
        await tracker_service.handle_peer_catalog_updated(sample_peer_catalog_updated)

        # Verify redis_service.update_peer_catalog was called
        redis_service.update_peer_catalog.assert_called_once_with(
            sample_peer_catalog_updated.peer_id,
            sample_peer_catalog_updated.catalog_ids,
            sample_peer_catalog_updated.is_full_update
        )

        # Verify _update_catalog_availability was called for each affected catalog ID
        assert tracker_service._update_catalog_availability.call_count == 2
        tracker_service._update_catalog_availability.assert_any_call("new-id-1")
        tracker_service._update_catalog_availability.assert_any_call("removed-id-1")

    @pytest.mark.asyncio
    async def test_update_catalog_availability(self, tracker_service, redis_service, kafka_service,
                                               subscription_service, test_data, sample_peer_info):
        """Test updating catalog availability status."""
        # Mock redis_service.get_peer_info_for_catalog to return peers
        redis_service.get_peer_info_for_catalog = MagicMock(return_value=[sample_peer_info])

        # Mock subscription_service.check_catalog_subscriptions
        subscription_service.check_catalog_subscriptions = MagicMock(return_value=1)

        # Mock kafka_service.publish_catalog_availability_changed
        kafka_service.publish_catalog_availability_changed = MagicMock()

        # Call method
        await tracker_service._update_catalog_availability(test_data["catalog_id"])

        # Verify redis_service.get_peer_info_for_catalog was called
        assert redis_service.get_peer_info_for_catalog.call_count == 1
        assert redis_service.get_peer_info_for_catalog.call_args[0][0] == test_data["catalog_id"]

        # Verify kafka_service.publish_catalog_availability_changed was called
        assert kafka_service.publish_catalog_availability_changed.call_count == 1
        event = kafka_service.publish_catalog_availability_changed.call_args[0][0]
        assert isinstance(event, CatalogAvailabilityChanged)
        assert event.catalog_id == test_data["catalog_id"]
        assert event.available_peer_count == 1
        assert event.is_available is True

        # Verify subscription_service.check_catalog_subscriptions was called
        assert subscription_service.check_catalog_subscriptions.call_count == 1
        assert subscription_service.check_catalog_subscriptions.call_args[0][0] == test_data["catalog_id"]
        assert subscription_service.check_catalog_subscriptions.call_args[0][1] == [sample_peer_info]

    @pytest.mark.asyncio
    async def test_update_catalog_availability_no_peers(self, tracker_service, redis_service, kafka_service,
                                                        subscription_service, test_data):
        """Test updating catalog availability when no peers are available."""
        # Mock redis_service.get_peer_info_for_catalog to return empty list
        redis_service.get_peer_info_for_catalog = MagicMock(return_value=[])

        # Mock kafka_service.publish_catalog_availability_changed
        kafka_service.publish_catalog_availability_changed = MagicMock()
        
        # Mock check_catalog_subscriptions method
        subscription_service.check_catalog_subscriptions = MagicMock()

        # Call method
        await tracker_service._update_catalog_availability(test_data["catalog_id"])

        # Verify redis_service.get_peer_info_for_catalog was called
        assert redis_service.get_peer_info_for_catalog.call_count == 1
        assert redis_service.get_peer_info_for_catalog.call_args[0][0] == test_data["catalog_id"]

        # Verify kafka_service.publish_catalog_availability_changed was called
        assert kafka_service.publish_catalog_availability_changed.call_count == 1
        event = kafka_service.publish_catalog_availability_changed.call_args[0][0]
        assert isinstance(event, CatalogAvailabilityChanged)
        assert event.catalog_id == test_data["catalog_id"]
        assert event.available_peer_count == 0
        assert event.is_available is False

        # Verify subscription_service.check_catalog_subscriptions was not called
        assert subscription_service.check_catalog_subscriptions.call_count == 0

    @pytest.mark.asyncio
    async def test_heartbeat_check_task(self, tracker_service, redis_service):
        """Test the heartbeat check task."""
        # Create a mock for asyncio.sleep to avoid waiting
        with patch("asyncio.sleep", AsyncMock()):
            # Mock find_stale_peers to return stale peers on first call, empty on second
            stale_peer = PeerInfo(
                peer_id="stale-peer-id",
                edge_id="stale-edge-id",
                state=PeerConnectionState.STALE
            )
            redis_service.find_stale_peers = MagicMock(side_effect=[[stale_peer], []])

            # Mock handle_peer_disconnected
            tracker_service.handle_peer_disconnected = AsyncMock()

            # Setup to break the infinite loop after one iteration
            counter = 0
            original_sleep = asyncio.sleep

            async def mock_sleep(seconds):
                nonlocal counter
                counter += 1
                if counter >= 1:
                    # Raise an exception to break the loop after first iteration
                    raise asyncio.CancelledError()
                await original_sleep(0.01)  # Short sleep for testing

            # Use our custom mock_sleep
            with patch("asyncio.sleep", mock_sleep):
                try:
                    # Call the task and it should exit after one iteration
                    await tracker_service._heartbeat_check_task()
                except asyncio.CancelledError:
                    pass

            # Verify redis_service.find_stale_peers was called
            redis_service.find_stale_peers.assert_called_once()

            # Verify handle_peer_disconnected was called for the stale peer
            tracker_service.handle_peer_disconnected.assert_called_once()
            event = tracker_service.handle_peer_disconnected.call_args[0][0]
            assert event.peer_id == "stale-peer-id"
            assert event.edge_id == "stale-edge-id"

    @pytest.mark.asyncio
    async def test_subscription_cleanup_task(self, tracker_service, subscription_service):
        """Test the subscription cleanup task."""
        # Mock cleanup_expired_subscriptions
        subscription_service.cleanup_expired_subscriptions = MagicMock(return_value=5)

        # Setup to break the infinite loop after one iteration
        counter = 0
        original_sleep = asyncio.sleep

        async def mock_sleep(seconds):
            nonlocal counter
            counter += 1
            if counter >= 1:
                # Raise an exception to break the loop after first iteration
                raise asyncio.CancelledError()
            await original_sleep(0.01)  # Short sleep for testing

        # Use our custom mock_sleep
        with patch("asyncio.sleep", mock_sleep):
            try:
                # Call the task and it should exit after one iteration
                await tracker_service._subscription_cleanup_task()
            except asyncio.CancelledError:
                pass

        # Verify subscription_service.cleanup_expired_subscriptions was called
        subscription_service.cleanup_expired_subscriptions.assert_called_once()

    def test_get_peers_for_catalog(self, tracker_service, redis_service, test_data):
        """Test getting peers for a catalog ID."""
        # Mock redis_service.get_peer_info_for_catalog
        redis_service.get_peer_info_for_catalog = MagicMock()
        
        # Call the method
        tracker_service.get_peers_for_catalog(test_data["catalog_id"])

        # Verify redis_service.get_peer_info_for_catalog was called
        assert redis_service.get_peer_info_for_catalog.call_count == 1
        assert redis_service.get_peer_info_for_catalog.call_args[0][0] == test_data["catalog_id"]

    def test_get_catalog_ids_for_peer(self, tracker_service, redis_service, test_data):
        """Test getting catalog IDs for a peer."""
        # Mock redis_service.get_peer_catalog_ids
        redis_service.get_peer_catalog_ids = MagicMock()
        
        # Call the method
        tracker_service.get_catalog_ids_for_peer(test_data["peer_id"])

        # Verify redis_service.get_peer_catalog_ids was called
        assert redis_service.get_peer_catalog_ids.call_count == 1
        assert redis_service.get_peer_catalog_ids.call_args[0][0] == test_data["peer_id"]

    def test_create_subscription(self, tracker_service, subscription_service, test_data):
        """Test creating a subscription."""
        # Mock subscription_service.create_subscription to return a subscription
        sample_subscription = MagicMock()
        sample_subscription.subscription_id = test_data["subscription_id"]
        subscription_service.create_subscription = MagicMock(return_value=sample_subscription)

        # Call the method
        result = tracker_service.create_subscription(
            test_data["service_id"],
            test_data["catalog_id"]
        )

        # Check the result
        assert result == test_data["subscription_id"]

        # Verify subscription_service.create_subscription was called
        subscription_service.create_subscription.assert_called_once_with(
            test_data["service_id"],
            test_data["catalog_id"],
            None
        )

    def test_create_subscription_failed(self, tracker_service, subscription_service, test_data):
        """Test creating a subscription that fails."""
        # Mock subscription_service.create_subscription to return None
        subscription_service.create_subscription = MagicMock(return_value=None)

        # Call the method
        result = tracker_service.create_subscription(
            test_data["service_id"],
            test_data["catalog_id"]
        )

        # Check the result
        assert result is None

        # Verify subscription_service.create_subscription was called
        subscription_service.create_subscription.assert_called_once_with(
            test_data["service_id"],
            test_data["catalog_id"],
            None
        )

    @pytest.mark.asyncio
    async def test_forward_request_not_implemented(self, tracker_service, test_data):
        """Test forwarding a request to a peer (not implemented yet)."""
        # Call the method
        success, payload, error = await tracker_service.forward_request(
            test_data["request_id"],
            test_data["peer_id"],
            "test-request-type",
            b"test-payload"
        )

        # Check the result
        assert success is False
        assert payload is None
        assert error == "Not implemented"
