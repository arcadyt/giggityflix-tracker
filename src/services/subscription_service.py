import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Set

from src.config import config
from src.models import (
    CatalogSubscription, CatalogSubscriptionMatched,
    PeerInfo, SubscriptionStatus
)
from src.services.kafka_service import KafkaService
from src.services.redis_service import RedisService

logger = logging.getLogger(__name__)


class SubscriptionService:
    def __init__(self, redis_service: RedisService, kafka_service: KafkaService):
        self.redis_service = redis_service
        self.kafka_service = kafka_service
        self.service_config = config.service

    def create_subscription(self, service_id: str, catalog_id: str, 
                            expiration_seconds: Optional[int] = None) -> Optional[CatalogSubscription]:
        """
        Create a new subscription for a catalog ID.
        Returns the created subscription or None if creation failed.
        """
        # Generate a unique subscription ID
        subscription_id = str(uuid.uuid4())
        
        # Set expiration time
        if expiration_seconds is None:
            expiration_seconds = self.service_config.subscription_default_ttl_hours * 3600
            
        expires_at = datetime.now() + timedelta(seconds=expiration_seconds)
        
        # Create subscription object
        subscription = CatalogSubscription(
            subscription_id=subscription_id,
            service_id=service_id,
            catalog_id=catalog_id,
            expires_at=expires_at,
            status=SubscriptionStatus.ACTIVE
        )
        
        # Save to Redis
        success = self.redis_service.create_subscription(subscription)
        if not success:
            logger.error(f"Failed to create subscription for service {service_id}, catalog {catalog_id}")
            return None
        
        logger.info(f"Created subscription {subscription_id} for service {service_id}, catalog {catalog_id}")
        
        # Check if catalog is already available
        self.check_subscription_fulfillment(subscription)
        
        return subscription

    def check_subscription_fulfillment(self, subscription: CatalogSubscription) -> bool:
        """
        Check if a subscription can be fulfilled now (catalog has peers).
        If so, send notification and update subscription status.
        Returns True if fulfilled, False otherwise.
        """
        # Skip if already fulfilled
        if subscription.status != SubscriptionStatus.ACTIVE:
            return False
            
        # Get all available peers for the catalog
        peers = self.redis_service.get_peer_info_for_catalog(subscription.catalog_id)
        
        # If no peers, can't fulfill
        if not peers:
            return False
            
        # Get peers that haven't been notified yet
        new_peers = [p for p in peers if p.peer_id not in subscription.notified_peer_ids]
        
        # If no new peers, no need to notify
        if not new_peers:
            return False
            
        # Update notified peers
        for peer in new_peers:
            self.redis_service.add_notified_peer(subscription.subscription_id, peer.peer_id)
            
        # Send notification
        event = CatalogSubscriptionMatched(
            subscription_id=subscription.subscription_id,
            service_id=subscription.service_id,
            catalog_id=subscription.catalog_id,
            available_peers=new_peers
        )
        
        self.kafka_service.publish_catalog_subscription_matched(event)
        
        logger.info(f"Fulfilled subscription {subscription.subscription_id} with {len(new_peers)} peers")
        return True

    def check_catalog_subscriptions(self, catalog_id: str, available_peers: List[PeerInfo]) -> int:
        """
        Check all subscriptions for a catalog ID against newly available peers.
        Returns count of fulfilled subscriptions.
        """
        # Get all active subscriptions for this catalog
        subscriptions = self.redis_service.get_subscriptions_for_catalog(catalog_id)
        
        fulfilled_count = 0
        for subscription in subscriptions:
            # Skip non-active subscriptions
            if subscription.status != SubscriptionStatus.ACTIVE:
                continue
                
            # Get peer IDs that haven't been notified
            notified_peer_ids = subscription.notified_peer_ids
            new_peers = [p for p in available_peers if p.peer_id not in notified_peer_ids]
            
            if not new_peers:
                continue
                
            # Update notified peers
            for peer in new_peers:
                self.redis_service.add_notified_peer(subscription.subscription_id, peer.peer_id)
                
            # Send notification
            event = CatalogSubscriptionMatched(
                subscription_id=subscription.subscription_id,
                service_id=subscription.service_id,
                catalog_id=subscription.catalog_id,
                available_peers=new_peers
            )
            
            self.kafka_service.publish_catalog_subscription_matched(event)
            fulfilled_count += 1
            
        if fulfilled_count > 0:
            logger.info(f"Fulfilled {fulfilled_count} subscriptions for catalog {catalog_id}")
            
        return fulfilled_count

    def get_subscription(self, subscription_id: str) -> Optional[CatalogSubscription]:
        """Get a subscription by ID."""
        return self.redis_service.get_subscription(subscription_id)

    def get_subscriptions_for_service(self, service_id: str) -> List[CatalogSubscription]:
        """Get all subscriptions for a service."""
        return self.redis_service.get_subscriptions_for_service(service_id)

    def cleanup_expired_subscriptions(self) -> int:
        """Clean up expired subscriptions. Returns count of cleaned subscriptions."""
        return self.redis_service.clear_expired_subscriptions()
