from datetime import datetime
from enum import Enum
from typing import List, Optional, Set

from pydantic import BaseModel, Field


class PeerConnectionState(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    STALE = "stale"  # Connection timeout exceeded but no disconnect event received


class PeerInfo(BaseModel):
    peer_id: str
    edge_id: str
    state: PeerConnectionState = PeerConnectionState.CONNECTED
    connected_at: datetime = Field(default_factory=datetime.now)
    last_heartbeat: datetime = Field(default_factory=datetime.now)
    disconnected_at: Optional[datetime] = None


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    FULFILLED = "fulfilled"
    EXPIRED = "expired"


class CatalogSubscription(BaseModel):
    subscription_id: str
    service_id: str
    catalog_id: str
    created_at: datetime = Field(default_factory=datetime.now)
    expires_at: datetime
    status: SubscriptionStatus = SubscriptionStatus.ACTIVE
    notified_peer_ids: Set[str] = Field(default_factory=set)


class CatalogAvailabilityChanged(BaseModel):
    catalog_id: str
    available_peer_count: int
    is_available: bool  # True if peer count > 0, False otherwise
    timestamp: datetime = Field(default_factory=datetime.now)


class CatalogSubscriptionMatched(BaseModel):
    subscription_id: str
    service_id: str
    catalog_id: str
    available_peers: List[PeerInfo]
    timestamp: datetime = Field(default_factory=datetime.now)


class PeerConnected(BaseModel):
    peer_id: str
    edge_id: str
    catalog_ids: List[str]
    timestamp: datetime = Field(default_factory=datetime.now)


class PeerDisconnected(BaseModel):
    peer_id: str
    edge_id: str
    timestamp: datetime = Field(default_factory=datetime.now)


class PeerCatalogUpdated(BaseModel):
    peer_id: str
    catalog_ids: List[str]
    is_full_update: bool = True  # True if this is the full catalog list, False for incremental
    timestamp: datetime = Field(default_factory=datetime.now)


class ForwardedRequest(BaseModel):
    request_id: str
    peer_id: str
    request_type: str
    payload: bytes
    timestamp: datetime = Field(default_factory=datetime.now)


class ForwardedResponse(BaseModel):
    request_id: str
    success: bool
    payload: Optional[bytes] = None
    error: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
