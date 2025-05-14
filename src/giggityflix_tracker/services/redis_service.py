import logging
from datetime import datetime, timedelta
from typing import List, Optional, Set, Tuple

from redis import Redis

from giggityflix_tracker.config import config
from giggityflix_tracker.models import (
    CatalogSubscription, PeerConnectionState,
    PeerInfo, SubscriptionStatus
)

logger = logging.getLogger(__name__)


class RedisService:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client
        self.redis_config = config.redis
        self.service_config = config.service

    # === Peer connection management ===

    def register_peer(self, peer_id: str, edge_id: str) -> None:
        """Register a peer connection with its edge server."""
        # Store peer to edge mapping
        peer_key = f"{self.redis_config.peer_edge_prefix}{peer_id}"

        # Store peer info as a hash
        now = datetime.now().isoformat()
        peer_info = {
            "peer_id": peer_id,
            "edge_id": edge_id,
            "state": PeerConnectionState.CONNECTED.value,
            "connected_at": now,
            "last_heartbeat": now,
            "disconnected_at": ""
        }

        # Use pipeline for atomic operations
        with self.redis.pipeline() as pipe:
            pipe.hmset(peer_key, peer_info)
            # Set TTL based on connection timeout to automatically clean up
            timeout = self.service_config.connection_timeout_seconds
            pipe.expire(peer_key, timeout + 60)  # Add 60s buffer
            pipe.execute()

        logger.info(f"Peer {peer_id} registered with edge {edge_id}")

    def update_peer_heartbeat(self, peer_id: str) -> bool:
        """Update last heartbeat timestamp for a peer. Returns True if successful."""
        peer_key = f"{self.redis_config.peer_edge_prefix}{peer_id}"

        # Check if peer exists
        if not self.redis.exists(peer_key):
            logger.warning(f"Attempted to update heartbeat for unknown peer {peer_id}")
            return False

        # Update last_heartbeat
        now = datetime.now().isoformat()
        with self.redis.pipeline() as pipe:
            pipe.hset(peer_key, "last_heartbeat", now)
            # Reset expiration
            timeout = self.service_config.connection_timeout_seconds
            pipe.expire(peer_key, timeout + 60)
            pipe.execute()

        return True

    def disconnect_peer(self, peer_id: str) -> Optional[PeerInfo]:
        """
        Mark a peer as disconnected and return its previous state.
        Returns None if peer wasn't found.
        """
        peer_key = f"{self.redis_config.peer_edge_prefix}{peer_id}"

        # Get current peer info
        peer_data = self.redis.hgetall(peer_key)
        if not peer_data:
            logger.warning(f"Attempted to disconnect unknown peer {peer_id}")
            return None

        # Create peer info object
        try:
            peer_info = PeerInfo(
                peer_id=peer_data["peer_id"],
                edge_id=peer_data["edge_id"],
                state=PeerConnectionState(peer_data["state"]),
                connected_at=datetime.fromisoformat(peer_data["connected_at"]),
                last_heartbeat=datetime.fromisoformat(peer_data["last_heartbeat"]),
                disconnected_at=datetime.fromisoformat(peer_data["disconnected_at"])
                if peer_data.get("disconnected_at") and peer_data["disconnected_at"] != "" else None
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing peer data for {peer_id}: {e}")
            return None

        # Update peer state
        now = datetime.now().isoformat()
        with self.redis.pipeline() as pipe:
            pipe.hmset(peer_key, {
                "state": PeerConnectionState.DISCONNECTED.value,
                "disconnected_at": now
            })
            # Keep record for a while but eventually clean up
            pipe.expire(peer_key, 86400)  # 24 hours
            pipe.execute()

        # Update the model with new state
        peer_info.state = PeerConnectionState.DISCONNECTED
        peer_info.disconnected_at = datetime.now()

        logger.info(f"Peer {peer_id} marked as disconnected")
        return peer_info

    def get_peer_info(self, peer_id: str) -> Optional[PeerInfo]:
        """Get information about a peer."""
        peer_key = f"{self.redis_config.peer_edge_prefix}{peer_id}"

        peer_data = self.redis.hgetall(peer_key)
        if not peer_data:
            return None

        try:
            return PeerInfo(
                peer_id=peer_data["peer_id"],
                edge_id=peer_data["edge_id"],
                state=PeerConnectionState(peer_data["state"]),
                connected_at=datetime.fromisoformat(peer_data["connected_at"]),
                last_heartbeat=datetime.fromisoformat(peer_data["last_heartbeat"]),
                disconnected_at=datetime.fromisoformat(peer_data["disconnected_at"])
                if peer_data.get("disconnected_at") and peer_data["disconnected_at"] != "" else None
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing peer data for {peer_id}: {e}")
            return None

    def find_stale_peers(self) -> List[PeerInfo]:
        """Find peers that haven't sent a heartbeat recently but aren't disconnected."""
        stale_peers = []
        pattern = f"{self.redis_config.peer_edge_prefix}*"

        peer_keys = self.redis.keys(pattern)
        now = datetime.now()
        timeout = timedelta(seconds=self.service_config.connection_timeout_seconds)

        for key in peer_keys:
            peer_data = self.redis.hgetall(key)

            # Skip if already disconnected
            if peer_data.get("state") == PeerConnectionState.DISCONNECTED.value:
                continue

            # Check if heartbeat is stale
            try:
                last_heartbeat = datetime.fromisoformat(peer_data.get("last_heartbeat", ""))
                if (now - last_heartbeat) > timeout:
                    # Create peer info object
                    peer_info = PeerInfo(
                        peer_id=peer_data["peer_id"],
                        edge_id=peer_data["edge_id"],
                        state=PeerConnectionState.STALE,
                        connected_at=datetime.fromisoformat(peer_data["connected_at"]),
                        last_heartbeat=last_heartbeat,
                        disconnected_at=None
                    )
                    stale_peers.append(peer_info)
            except (KeyError, ValueError) as e:
                logger.error(f"Error checking staleness for {key}: {e}")

        return stale_peers

    # === Catalog management ===

    def update_peer_catalog(self, peer_id: str, catalog_ids: List[str], is_full_update: bool = True) -> Tuple[
        Set[str], Set[str]]:
        """
        Update the catalog IDs associated with a peer.
        Returns tuple of (added_catalog_ids, removed_catalog_ids).
        For full updates, compares with previous catalog IDs.
        For incremental updates, just adds the new IDs.
        """
        peer_info = self.get_peer_info(peer_id)
        if not peer_info or peer_info.state == PeerConnectionState.DISCONNECTED:
            logger.warning(f"Cannot update catalog for disconnected or unknown peer {peer_id}")
            return set(), set()

        # Get existing catalog IDs for this peer
        peer_catalogs_key = f"{self.redis_config.peer_catalogs_prefix}{peer_id}"
        existing_catalog_ids = set(self.redis.smembers(peer_catalogs_key) or [])

        # For full updates, calculate difference
        if is_full_update:
            new_catalog_ids = set(catalog_ids)
            added_ids = new_catalog_ids - existing_catalog_ids
            removed_ids = existing_catalog_ids - new_catalog_ids

            # Use pipeline for atomic update
            with self.redis.pipeline() as pipe:
                # Remove old mapping
                if removed_ids:
                    pipe.srem(peer_catalogs_key, *removed_ids)
                    # Remove peer from catalog-to-peers mappings
                    for catalog_id in removed_ids:
                        catalog_peers_key = f"{self.redis_config.catalog_peers_prefix}{catalog_id}"
                        pipe.srem(catalog_peers_key, peer_id)

                # Add new mapping
                if added_ids:
                    pipe.sadd(peer_catalogs_key, *added_ids)
                    # Add peer to catalog-to-peers mappings
                    for catalog_id in added_ids:
                        catalog_peers_key = f"{self.redis_config.catalog_peers_prefix}{catalog_id}"
                        pipe.sadd(catalog_peers_key, peer_id)

                pipe.execute()
        else:
            # For incremental updates, just add
            added_ids = set(catalog_ids) - existing_catalog_ids
            removed_ids = set()

            if added_ids:
                with self.redis.pipeline() as pipe:
                    pipe.sadd(peer_catalogs_key, *added_ids)
                    for catalog_id in added_ids:
                        catalog_peers_key = f"{self.redis_config.catalog_peers_prefix}{catalog_id}"
                        pipe.sadd(catalog_peers_key, peer_id)
                    pipe.execute()

        return added_ids, removed_ids

    def remove_peer_catalog(self, peer_id: str) -> Set[str]:
        """
        Remove all catalog associations for a peer (on disconnect).
        Returns the set of catalog IDs that were removed.
        """
        peer_catalogs_key = f"{self.redis_config.peer_catalogs_prefix}{peer_id}"
        catalog_ids = set(self.redis.smembers(peer_catalogs_key) or [])

        if not catalog_ids:
            return set()

        # Use pipeline for atomic update
        with self.redis.pipeline() as pipe:
            # Remove peer-to-catalogs mapping
            pipe.delete(peer_catalogs_key)

            # Remove peer from catalog-to-peers mappings
            for catalog_id in catalog_ids:
                catalog_peers_key = f"{self.redis_config.catalog_peers_prefix}{catalog_id}"
                pipe.srem(catalog_peers_key, peer_id)

            pipe.execute()

        return catalog_ids

    def get_peer_catalog_ids(self, peer_id: str) -> List[str]:
        """Get all catalog IDs associated with a peer."""
        peer_catalogs_key = f"{self.redis_config.peer_catalogs_prefix}{peer_id}"
        return list(self.redis.smembers(peer_catalogs_key) or [])

    def get_peers_for_catalog(self, catalog_id: str) -> List[str]:
        """Get all peer IDs that have a specific catalog ID."""
        catalog_peers_key = f"{self.redis_config.catalog_peers_prefix}{catalog_id}"
        return list(self.redis.smembers(catalog_peers_key) or [])

    def get_peer_info_for_catalog(self, catalog_id: str) -> List[PeerInfo]:
        """Get detailed peer info for all peers that have a specific catalog ID."""
        peer_ids = self.get_peers_for_catalog(catalog_id)
        peers = []

        for peer_id in peer_ids:
            peer_info = self.get_peer_info(peer_id)
            if peer_info and peer_info.state == PeerConnectionState.CONNECTED:
                peers.append(peer_info)

        return peers

    # === Subscription management ===

    def create_subscription(self, subscription: CatalogSubscription) -> bool:
        """Create a new catalog subscription."""
        sub_key = f"{self.redis_config.subscription_prefix}{subscription.subscription_id}"

        # Check if already exists
        if self.redis.exists(sub_key):
            logger.warning(f"Subscription {subscription.subscription_id} already exists")
            return False

        # Store subscription data
        sub_data = subscription.model_dump_json()

        # Store by ID and add to service and catalog indexes
        service_subs_key = f"{self.redis_config.subscription_prefix}service:{subscription.service_id}"
        catalog_subs_key = f"{self.redis_config.subscription_prefix}catalog:{subscription.catalog_id}"

        with self.redis.pipeline() as pipe:
            pipe.set(sub_key, sub_data)
            pipe.sadd(service_subs_key, subscription.subscription_id)
            pipe.sadd(catalog_subs_key, subscription.subscription_id)

            # Set expiration
            ttl_seconds = int((subscription.expires_at - datetime.now()).total_seconds())
            if ttl_seconds > 0:
                pipe.expire(sub_key, ttl_seconds)
                # We don't expire the index sets as they're shared across subscriptions

            pipe.execute()

        return True

    def get_subscription(self, subscription_id: str) -> Optional[CatalogSubscription]:
        """Get a subscription by ID."""
        sub_key = f"{self.redis_config.subscription_prefix}{subscription_id}"
        sub_data = self.redis.get(sub_key)

        if not sub_data:
            return None

        try:
            subscription = CatalogSubscription.model_validate_json(sub_data)
            return subscription
        except Exception as e:
            logger.error(f"Error parsing subscription {subscription_id}: {e}")
            return None

    def update_subscription(self, subscription: CatalogSubscription) -> bool:
        """Update a subscription."""
        sub_key = f"{self.redis_config.subscription_prefix}{subscription.subscription_id}"

        # Check if exists
        if not self.redis.exists(sub_key):
            logger.warning(f"Cannot update non-existent subscription {subscription.subscription_id}")
            return False

        # Store updated data
        sub_data = subscription.model_dump_json()

        with self.redis.pipeline() as pipe:
            pipe.set(sub_key, sub_data)

            # Update expiration if needed
            ttl_seconds = int((subscription.expires_at - datetime.now()).total_seconds())
            if ttl_seconds > 0:
                pipe.expire(sub_key, ttl_seconds)

            pipe.execute()

        return True

    def get_subscriptions_for_catalog(self, catalog_id: str) -> List[CatalogSubscription]:
        """Get all active subscriptions for a catalog ID."""
        catalog_subs_key = f"{self.redis_config.subscription_prefix}catalog:{catalog_id}"
        subscription_ids = self.redis.smembers(catalog_subs_key) or []

        subscriptions = []
        for sub_id in subscription_ids:
            subscription = self.get_subscription(sub_id)
            if subscription and subscription.status == SubscriptionStatus.ACTIVE:
                subscriptions.append(subscription)

        return subscriptions

    def get_subscriptions_for_service(self, service_id: str) -> List[CatalogSubscription]:
        """Get all subscriptions for a service ID."""
        service_subs_key = f"{self.redis_config.subscription_prefix}service:{service_id}"
        subscription_ids = self.redis.smembers(service_subs_key) or []

        subscriptions = []
        for sub_id in subscription_ids:
            subscription = self.get_subscription(sub_id)
            if subscription:
                subscriptions.append(subscription)

        return subscriptions

    def add_notified_peer(self, subscription_id: str, peer_id: str) -> bool:
        """Add a peer to the list of peers the subscription has been notified about."""
        subscription = self.get_subscription(subscription_id)
        if not subscription:
            return False

        # Add peer to notified set
        subscription.notified_peer_ids.add(peer_id)

        # Update subscription
        return self.update_subscription(subscription)

    def clear_expired_subscriptions(self) -> int:
        """
        Clear expired subscriptions. Returns count of cleared subscriptions.
        Redis TTL handles expiration, this is just for index cleanup.
        """
        now = datetime.now()
        pattern = f"{self.redis_config.subscription_prefix}[^:]*"  # Match subscription IDs directly

        # Get all subscription keys
        subscription_keys = self.redis.keys(pattern)
        expired_count = 0

        for key in subscription_keys:
            # Check if expired
            sub_data = self.redis.get(key)
            if not sub_data:
                continue

            try:
                subscription = CatalogSubscription.model_validate_json(sub_data)

                if subscription.expires_at <= now or subscription.status == SubscriptionStatus.EXPIRED:
                    # Clean up indexes
                    service_subs_key = f"{self.redis_config.subscription_prefix}service:{subscription.service_id}"
                    catalog_subs_key = f"{self.redis_config.subscription_prefix}catalog:{subscription.catalog_id}"

                    with self.redis.pipeline() as pipe:
                        pipe.srem(service_subs_key, subscription.subscription_id)
                        pipe.srem(catalog_subs_key, subscription.subscription_id)
                        pipe.execute()

                    expired_count += 1
            except Exception as e:
                logger.error(f"Error processing subscription {key}: {e}")

        return expired_count
