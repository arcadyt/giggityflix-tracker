import os

from pydantic import BaseModel, Field


class GrpcConfig(BaseModel):
    address: str = Field(default_factory=lambda: os.environ.get("GRPC_ADDRESS", "0.0.0.0:50051"))
    max_workers: int = Field(default_factory=lambda: int(os.environ.get("GRPC_MAX_WORKERS", "10")))
    use_tls: bool = Field(default_factory=lambda: os.environ.get("GRPC_USE_TLS", "false").lower() == "true")


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    group_id: str = Field(default_factory=lambda: os.environ.get("KAFKA_GROUP_ID", "tracker-service"))
    
    # Topics for consuming
    peer_connected_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_PEER_CONNECTED_TOPIC", "peer.connection.established"))
    peer_disconnected_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_PEER_DISCONNECTED_TOPIC", "peer.connection.closed"))
    peer_catalog_updated_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_PEER_CATALOG_UPDATED_TOPIC", "peer.catalog.updated"))
    
    # Topics for publishing
    catalog_availability_changed_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_CATALOG_AVAILABILITY_CHANGED_TOPIC", "catalog.availability.changed"))
    catalog_subscription_matched_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_CATALOG_SUBSCRIPTION_MATCHED_TOPIC", "catalog.subscription.matched"))


class RedisConfig(BaseModel):
    host: str = Field(default_factory=lambda: os.environ.get("REDIS_HOST", "localhost"))
    port: int = Field(default_factory=lambda: int(os.environ.get("REDIS_PORT", "6379")))
    db: int = Field(default_factory=lambda: int(os.environ.get("REDIS_DB", "0")))
    password: str = Field(default_factory=lambda: os.environ.get("REDIS_PASSWORD", ""))
    
    # Key prefixes for Redis
    peer_edge_prefix: str = Field(
        default_factory=lambda: os.environ.get("REDIS_PEER_EDGE_PREFIX", "tracker:peer:edge:"))
    catalog_peers_prefix: str = Field(
        default_factory=lambda: os.environ.get("REDIS_CATALOG_PEERS_PREFIX", "tracker:catalog:peers:"))
    peer_catalogs_prefix: str = Field(
        default_factory=lambda: os.environ.get("REDIS_PEER_CATALOGS_PREFIX", "tracker:peer:catalogs:"))
    subscription_prefix: str = Field(
        default_factory=lambda: os.environ.get("REDIS_SUBSCRIPTION_PREFIX", "tracker:subscription:"))


class ServiceConfig(BaseModel):
    heartbeat_interval_seconds: int = Field(
        default_factory=lambda: int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "30")))
    connection_timeout_seconds: int = Field(
        default_factory=lambda: int(os.environ.get("CONNECTION_TIMEOUT_SECONDS", "120")))
    subscription_default_ttl_hours: int = Field(
        default_factory=lambda: int(os.environ.get("SUBSCRIPTION_DEFAULT_TTL_HOURS", "24")))


class AppConfig(BaseModel):
    grpc: GrpcConfig = Field(default_factory=GrpcConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)


# Singleton config instance
config = AppConfig()
