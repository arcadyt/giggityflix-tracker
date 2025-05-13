from freezegun import freeze_time

from tests.conftest import *  # Import fixtures


@pytest.mark.unit
class TestRedisService:

    # ===== Peer Connection Management Tests =====

    def test_register_peer(self, redis_service, mock_redis, test_data):
        """Test registering a new peer connection."""
        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Freeze time for predictable timestamps
        with freeze_time("2025-05-13 12:00:00"):
            # Call method
            redis_service.register_peer(test_data["peer_id"], test_data["edge_id"])

            # Check Redis pipeline was used
            mock_redis.pipeline.assert_called_once()

            # Verify hmset was called with correct data in the pipeline
            pipeline_mock.hmset.assert_called_once()
            args, kwargs = pipeline_mock.hmset.call_args

            # Check first arg is the correct key
            expected_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
            assert args[0] == expected_key

            # Check second arg contains expected peer data
            peer_data = args[1]
            assert peer_data["peer_id"] == test_data["peer_id"]
            assert peer_data["edge_id"] == test_data["edge_id"]
            assert peer_data["state"] == PeerConnectionState.CONNECTED.value
            assert "connected_at" in peer_data
            assert "last_heartbeat" in peer_data

            # Verify expire was called
            pipeline_mock.expire.assert_called_once()

            # Verify execute was called
            pipeline_mock.execute.assert_called_once()

    def test_update_peer_heartbeat_existing(self, redis_service, mock_redis, test_data):
        """Test updating heartbeat for an existing peer."""
        # Setup mock to indicate peer exists
        mock_redis.exists.return_value = True

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Call method
        result = redis_service.update_peer_heartbeat(test_data["peer_id"])

        # Check result
        assert result is True

        # Verify Redis operations
        peer_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
        mock_redis.exists.assert_called_once_with(peer_key)

        # Verify hset was called in the pipeline
        pipeline_mock.hset.assert_called_once()
        args, kwargs = pipeline_mock.hset.call_args
        assert args[0] == peer_key
        assert args[1] == "last_heartbeat"
        assert isinstance(args[2], str)  # ISO datetime string

        # Verify TTL was extended in the pipeline
        pipeline_mock.expire.assert_called_once()

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

    def test_update_peer_heartbeat_nonexistent(self, redis_service, mock_redis, test_data):
        """Test updating heartbeat for a non-existent peer."""
        # Setup mock to indicate peer doesn't exist
        mock_redis.exists.return_value = False

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Call method
        result = redis_service.update_peer_heartbeat(test_data["peer_id"])

        # Check result
        assert result is False

        # Verify Redis operations - should check existence but not update
        peer_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
        mock_redis.exists.assert_called_once_with(peer_key)
        pipeline_mock.hset.assert_not_called()
        pipeline_mock.expire.assert_not_called()
        pipeline_mock.execute.assert_not_called()

    def test_disconnect_peer(self, redis_service, mock_redis, test_data, sample_peer_info):
        """Test marking a peer as disconnected."""
        # Setup mock to return peer data
        mock_redis.hgetall.return_value = {
            "peer_id": test_data["peer_id"],
            "edge_id": test_data["edge_id"],
            "state": PeerConnectionState.CONNECTED.value,
            "connected_at": datetime.now().isoformat(),
            "last_heartbeat": datetime.now().isoformat(),
            "disconnected_at": ""
        }

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Call method
        result = redis_service.disconnect_peer(test_data["peer_id"])

        # Check result is a PeerInfo
        assert isinstance(result, PeerInfo)
        assert result.peer_id == test_data["peer_id"]
        assert result.state == PeerConnectionState.DISCONNECTED

        # Verify Redis operations
        peer_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
        mock_redis.hgetall.assert_called_once_with(peer_key)

        # Verify hmset was called in the pipeline
        pipeline_mock.hmset.assert_called_once()
        args, kwargs = pipeline_mock.hmset.call_args
        assert args[0] == peer_key
        update_data = args[1]
        assert update_data["state"] == PeerConnectionState.DISCONNECTED.value
        assert "disconnected_at" in update_data

        # Verify 24hr TTL was set in the pipeline
        pipeline_mock.expire.assert_called_once_with(peer_key, 86400)

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

    def test_get_peer_info(self, redis_service, mock_redis, test_data):
        """Test retrieving peer information."""
        # Setup mock to return peer data
        now = datetime.now().isoformat()
        mock_redis.hgetall.return_value = {
            "peer_id": test_data["peer_id"],
            "edge_id": test_data["edge_id"],
            "state": PeerConnectionState.CONNECTED.value,
            "connected_at": now,
            "last_heartbeat": now,
            "disconnected_at": ""
        }

        # Call method
        result = redis_service.get_peer_info(test_data["peer_id"])

        # Check result
        assert isinstance(result, PeerInfo)
        assert result.peer_id == test_data["peer_id"]
        assert result.edge_id == test_data["edge_id"]
        assert result.state == PeerConnectionState.CONNECTED
        assert result.disconnected_at is None

        # Verify Redis operation
        peer_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
        mock_redis.hgetall.assert_called_once_with(peer_key)

    def test_find_stale_peers(self, redis_service, mock_redis, test_data):
        """Test finding peers that haven't sent heartbeats recently."""
        # Setup mock to return peer keys
        peer_key = f"{redis_service.redis_config.peer_edge_prefix}{test_data['peer_id']}"
        mock_redis.keys.return_value = [peer_key.encode('utf-8')]

        # Setup mock to return peer data with stale heartbeat
        now = datetime.now()
        stale_time = (now - timedelta(seconds=redis_service.service_config.connection_timeout_seconds + 10)).isoformat()
        mock_redis.hgetall.return_value = {
            "peer_id": test_data["peer_id"],
            "edge_id": test_data["edge_id"],
            "state": PeerConnectionState.CONNECTED.value,
            "connected_at": now.isoformat(),
            "last_heartbeat": stale_time,
            "disconnected_at": ""
        }

        # Call method
        result = redis_service.find_stale_peers()

        # Check result
        assert len(result) == 1
        assert result[0].peer_id == test_data["peer_id"]
        assert result[0].state == PeerConnectionState.STALE

        # Verify Redis operations
        pattern = f"{redis_service.redis_config.peer_edge_prefix}*"
        mock_redis.keys.assert_called_once_with(pattern)
        mock_redis.hgetall.assert_called_once_with(peer_key.encode('utf-8'))

    # ===== Catalog Management Tests =====

    def test_update_peer_catalog_full(self, redis_service, mock_redis, test_data):
        """Test full update of a peer's catalog IDs."""
        # Setup mocks
        # Mock get_peer_info to return connected peer
        redis_service.get_peer_info = MagicMock(return_value=PeerInfo(
            peer_id=test_data["peer_id"],
            edge_id=test_data["edge_id"],
            state=PeerConnectionState.CONNECTED
        ))

        # Mock existing catalog IDs
        peer_catalogs_key = f"{redis_service.redis_config.peer_catalogs_prefix}{test_data['peer_id']}"
        mock_redis.smembers.return_value = {"old-catalog-1", "old-catalog-2", test_data["catalog_id"]}

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # New catalog IDs to set
        new_catalog_ids = [test_data["catalog_id"], "new-catalog-1"]

        # Call method
        added_ids, removed_ids = redis_service.update_peer_catalog(
            test_data["peer_id"], new_catalog_ids, is_full_update=True
        )

        # Check result
        assert added_ids == {"new-catalog-1"}
        assert removed_ids == {"old-catalog-1", "old-catalog-2"}

        # Verify Redis operations for removed IDs in the pipeline
        pipeline_mock.srem.assert_any_call(peer_catalogs_key, *removed_ids)
        for catalog_id in removed_ids:
            catalog_peers_key = f"{redis_service.redis_config.catalog_peers_prefix}{catalog_id}"
            pipeline_mock.srem.assert_any_call(catalog_peers_key, test_data["peer_id"])

        # Verify Redis operations for added IDs in the pipeline
        pipeline_mock.sadd.assert_any_call(peer_catalogs_key, *added_ids)
        for catalog_id in added_ids:
            catalog_peers_key = f"{redis_service.redis_config.catalog_peers_prefix}{catalog_id}"
            pipeline_mock.sadd.assert_any_call(catalog_peers_key, test_data["peer_id"])

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

    def test_update_peer_catalog_incremental(self, redis_service, mock_redis, test_data):
        """Test incremental update of a peer's catalog IDs."""
        # Setup mocks
        # Mock get_peer_info to return connected peer
        redis_service.get_peer_info = MagicMock(return_value=PeerInfo(
            peer_id=test_data["peer_id"],
            edge_id=test_data["edge_id"],
            state=PeerConnectionState.CONNECTED
        ))

        # Mock existing catalog IDs
        peer_catalogs_key = f"{redis_service.redis_config.peer_catalogs_prefix}{test_data['peer_id']}"
        mock_redis.smembers.return_value = {test_data["catalog_id"]}

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # New catalog IDs to add
        new_catalog_ids = ["new-catalog-1"]

        # Call method
        added_ids, removed_ids = redis_service.update_peer_catalog(
            test_data["peer_id"], new_catalog_ids, is_full_update=False
        )

        # Check result
        assert added_ids == {"new-catalog-1"}
        assert removed_ids == set()  # No removals in incremental update

        # Verify Redis operations for added IDs in the pipeline
        pipeline_mock.sadd.assert_any_call(peer_catalogs_key, *added_ids)
        for catalog_id in added_ids:
            catalog_peers_key = f"{redis_service.redis_config.catalog_peers_prefix}{catalog_id}"
            pipeline_mock.sadd.assert_any_call(catalog_peers_key, test_data["peer_id"])

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

        # Verify no removals were performed
        assert not mock_redis.srem.called

    def test_remove_peer_catalog(self, redis_service, mock_redis, test_data):
        """Test removing all catalog associations for a peer."""
        # Setup mocks
        peer_catalogs_key = f"{redis_service.redis_config.peer_catalogs_prefix}{test_data['peer_id']}"
        catalog_ids = {test_data["catalog_id"], "another-catalog-id"}
        mock_redis.smembers.return_value = catalog_ids

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Call method
        result = redis_service.remove_peer_catalog(test_data["peer_id"])

        # Check result
        assert result == catalog_ids

        # Verify Redis operations
        mock_redis.smembers.assert_called_once_with(peer_catalogs_key)

        # Verify delete in the pipeline
        pipeline_mock.delete.assert_called_once_with(peer_catalogs_key)

        # Verify removal from catalog-to-peers mappings in the pipeline
        for catalog_id in catalog_ids:
            catalog_peers_key = f"{redis_service.redis_config.catalog_peers_prefix}{catalog_id}"
            pipeline_mock.srem.assert_any_call(catalog_peers_key, test_data["peer_id"])

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

    # ===== Subscription Management Tests =====

    def test_create_subscription(self, redis_service, mock_redis, sample_catalog_subscription):
        """Test creating a new catalog subscription."""
        # Setup mock to indicate subscription doesn't exist
        mock_redis.exists.return_value = False

        # Get the pipeline mock
        pipeline_mock = mock_redis.pipeline.return_value

        # Call method
        result = redis_service.create_subscription(sample_catalog_subscription)

        # Check result
        assert result is True

        # Verify Redis operations
        sub_key = f"{redis_service.redis_config.subscription_prefix}{sample_catalog_subscription.subscription_id}"
        mock_redis.exists.assert_called_once_with(sub_key)

        # Verify set in the pipeline
        pipeline_mock.set.assert_called_once()
        args, kwargs = pipeline_mock.set.call_args
        assert args[0] == sub_key

        # Verify JSON data
        sub_data = json.loads(args[1])
        assert sub_data["subscription_id"] == sample_catalog_subscription.subscription_id
        assert sub_data["service_id"] == sample_catalog_subscription.service_id
        assert sub_data["catalog_id"] == sample_catalog_subscription.catalog_id

        # Verify adding to indexes in the pipeline
        service_subs_key = f"{redis_service.redis_config.subscription_prefix}service:{sample_catalog_subscription.service_id}"
        catalog_subs_key = f"{redis_service.redis_config.subscription_prefix}catalog:{sample_catalog_subscription.catalog_id}"
        pipeline_mock.sadd.assert_any_call(service_subs_key, sample_catalog_subscription.subscription_id)
        pipeline_mock.sadd.assert_any_call(catalog_subs_key, sample_catalog_subscription.subscription_id)

        # Verify TTL was set in the pipeline if ttl_seconds > 0
        ttl_seconds = int((sample_catalog_subscription.expires_at - datetime.now()).total_seconds())
        if ttl_seconds > 0:
            # Use a more flexible assertion that allows for small timing differences (±1 second)
            expire_calls = pipeline_mock.expire.call_args_list
            assert any(
                call[0][0] == sub_key and abs(call[0][1] - ttl_seconds) <= 1
                for call in expire_calls
            ), f"Expected expire with key={sub_key} and TTL close to {ttl_seconds}"

        # Verify execute was called
        pipeline_mock.execute.assert_called_once()

    def test_get_subscription(self, redis_service, mock_redis, test_data, sample_catalog_subscription):
        """Test retrieving a subscription by ID."""
        # Setup mock to return subscription data
        sub_key = f"{redis_service.redis_config.subscription_prefix}{test_data['subscription_id']}"
        mock_redis.get.return_value = sample_catalog_subscription.model_dump_json()

        # Call method
        result = redis_service.get_subscription(test_data["subscription_id"])

        # Check result
        assert isinstance(result, type(sample_catalog_subscription))
        assert result.subscription_id == sample_catalog_subscription.subscription_id
        assert result.service_id == sample_catalog_subscription.service_id
        assert result.catalog_id == sample_catalog_subscription.catalog_id

        # Verify Redis operation
        mock_redis.get.assert_called_once_with(sub_key)

    def test_get_subscriptions_for_catalog(self, redis_service, mock_redis, test_data, sample_catalog_subscription):
        """Test getting all active subscriptions for a catalog ID."""
        # Setup mocks
        catalog_subs_key = f"{redis_service.redis_config.subscription_prefix}catalog:{test_data['catalog_id']}"
        mock_redis.smembers.return_value = {sample_catalog_subscription.subscription_id}

        # Mock get_subscription
        redis_service.get_subscription = MagicMock(return_value=sample_catalog_subscription)

        # Call method
        result = redis_service.get_subscriptions_for_catalog(test_data["catalog_id"])

        # Check result
        assert len(result) == 1
        assert result[0] == sample_catalog_subscription

        # Verify Redis operations
        mock_redis.smembers.assert_called_once_with(catalog_subs_key)
        redis_service.get_subscription.assert_called_once_with(sample_catalog_subscription.subscription_id)

    def test_add_notified_peer(self, redis_service, test_data, sample_catalog_subscription):
        """Test adding a peer to subscription's notified list."""
        # Mock get_subscription and update_subscription
        redis_service.get_subscription = MagicMock(return_value=sample_catalog_subscription)
        redis_service.update_subscription = MagicMock(return_value=True)

        # Call method
        result = redis_service.add_notified_peer(test_data["subscription_id"], test_data["peer_id"])

        # Check result
        assert result is True

        # Verify operations
        redis_service.get_subscription.assert_called_once_with(test_data["subscription_id"])

        # Verify peer was added to notified set
        updated_subscription = redis_service.update_subscription.call_args[0][0]
        assert test_data["peer_id"] in updated_subscription.notified_peer_ids
