import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Tuple

from src.config import config
from src.models import (
    CatalogAvailabilityChanged, PeerCatalogUpdated,
    PeerConnected, PeerDisconnected, PeerInfo
)
from src.services.kafka_service import KafkaService
from src.services.redis_service import RedisService
from src.services.subscription_service import SubscriptionService

logger = logging.getLogger(__name__)


class TrackerService:
    def __init__(
            self,
            redis_service: RedisService,
            kafka_service: KafkaService,
            subscription_service: SubscriptionService
    ):
        self.redis_service = redis_service
        self.kafka_service = kafka_service
        self.subscription_service = subscription_service
        self.service_config = config.service

        # Start maintenance tasks
        self._start_maintenance_tasks()

    def _start_maintenance_tasks(self) -> None:
        """Start background maintenance tasks."""
        # Create maintenance tasks
        asyncio.create_task(self._heartbeat_check_task())
        asyncio.create_task(self._subscription_cleanup_task())

        logger.info("Maintenance tasks started")

    async def _heartbeat_check_task(self) -> None:
        """Periodically check for stale peers."""
        while True:
            try:
                # Find stale peers
                stale_peers = self.redis_service.find_stale_peers()

                for peer in stale_peers:
                    # Handle as disconnection
                    logger.info(f"Peer {peer.peer_id} is stale, handling as disconnection")
                    await self.handle_peer_disconnected(PeerDisconnected(
                        peer_id=peer.peer_id,
                        edge_id=peer.edge_id,
                        timestamp=datetime.now()
                    ))

            except Exception as e:
                logger.error(f"Error in heartbeat check task: {e}")

            # Sleep before next check
            await asyncio.sleep(self.service_config.heartbeat_interval_seconds)

    async def _subscription_cleanup_task(self) -> None:
        """Periodically clean up expired subscriptions."""
        # Sleep for 1 hour between cleanups
        cleanup_interval = 3600

        while True:
            try:
                # Clean up expired subscriptions
                cleaned_count = self.subscription_service.cleanup_expired_subscriptions()
                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} expired subscriptions")

            except Exception as e:
                logger.error(f"Error in subscription cleanup task: {e}")

            # Sleep before next cleanup
            await asyncio.sleep(cleanup_interval)

    # === Event handlers ===

    async def handle_peer_connected(self, event: PeerConnected) -> None:
        """Handle peer.connection.established event."""
        logger.info(f"Handling peer connected event: {event.peer_id}")

        # Register peer
        self.redis_service.register_peer(event.peer_id, event.edge_id)

        # Process catalog IDs if provided
        if event.catalog_ids:
            await self.handle_peer_catalog_updated(PeerCatalogUpdated(
                peer_id=event.peer_id,
                catalog_ids=event.catalog_ids,
                is_full_update=True,
                timestamp=event.timestamp
            ))

    async def handle_peer_disconnected(self, event: PeerDisconnected) -> None:
        """Handle peer.connection.closed event."""
        logger.info(f"Handling peer disconnected event: {event.peer_id}")

        # Get peer info before disconnect for catalog cleanup
        peer_info = self.redis_service.get_peer_info(event.peer_id)
        if not peer_info:
            logger.warning(f"No info found for disconnected peer {event.peer_id}")
            return

        # Mark peer as disconnected
        self.redis_service.disconnect_peer(event.peer_id)

        # Remove catalog associations and update availability
        removed_catalog_ids = self.redis_service.remove_peer_catalog(event.peer_id)

        # Process catalog availability changes
        for catalog_id in removed_catalog_ids:
            await self._update_catalog_availability(catalog_id)

    async def handle_peer_catalog_updated(self, event: PeerCatalogUpdated) -> None:
        """Handle peer.catalog.updated event."""
        logger.info(f"Handling peer catalog updated event: {event.peer_id} with {len(event.catalog_ids)} catalog IDs")

        # Update catalog mappings
        added_ids, removed_ids = self.redis_service.update_peer_catalog(
            event.peer_id,
            event.catalog_ids,
            event.is_full_update
        )

        # Process all affected catalog IDs for availability changes
        affected_ids = added_ids.union(removed_ids)

        for catalog_id in affected_ids:
            await self._update_catalog_availability(catalog_id)

    async def _update_catalog_availability(self, catalog_id: str) -> None:
        """
        Update availability status for a catalog ID.
        Publishes events and checks subscriptions as needed.
        """
        # Get all peers with this catalog
        peers = self.redis_service.get_peer_info_for_catalog(catalog_id)
        peer_count = len(peers)

        # Determine if this is a change in availability
        is_available = peer_count > 0

        # Publish availability changed event
        event = CatalogAvailabilityChanged(
            catalog_id=catalog_id,
            available_peer_count=peer_count,
            is_available=is_available
        )

        self.kafka_service.publish_catalog_availability_changed(event)

        # If catalog is available, check subscriptions
        if is_available:
            self.subscription_service.check_catalog_subscriptions(catalog_id, peers)

    # === gRPC service methods ===

    def get_peers_for_catalog(self, catalog_id: str) -> List[PeerInfo]:
        """Get all connected peers for a catalog ID."""
        return self.redis_service.get_peer_info_for_catalog(catalog_id)

    def get_catalog_ids_for_peer(self, peer_id: str) -> List[str]:
        """Get all catalog IDs for a peer."""
        return self.redis_service.get_peer_catalog_ids(peer_id)

    def create_subscription(self, service_id: str, catalog_id: str,
                            expiration_seconds: Optional[int] = None) -> Optional[str]:
        """
        Create a subscription for a catalog ID.
        Returns the subscription ID or None if failed.
        """
        subscription = self.subscription_service.create_subscription(
            service_id, catalog_id, expiration_seconds
        )

        return subscription.subscription_id if subscription else None

    async def forward_request(self, request_id: str, peer_id: str,
                              request_type: str, payload: bytes) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """
        Forward a request to a peer via its edge service.
        Returns (success, response_payload, error_message).
        """
        # This method is a placeholder and needs to be implemented with gRPC client
        # to communicate with the edge service
        logger.error("forward_request not implemented yet")
        return False, None, "Not implemented"
