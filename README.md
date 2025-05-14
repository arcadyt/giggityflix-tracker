# Giggityflix Tracker Service

Microservice for tracking peer connections, routing requests, and managing catalog availability for the Giggityflix
media streaming platform.

## Overview

This service:

- Tracks peer to edge connections
- Maintains mappings of catalog IDs to peers
- Routes requests to peers via appropriate edge servers
- Provides catalog subscription mechanism
- Publishes catalog availability events

## Installation

```bash
# Install dependencies
poetry install

# Run service
poetry run python -m src.main
```

## Environment Variables

### Server Configuration
- TBD

### Kafka Configuration

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: localhost:9092)
- `KAFKA_GROUP_ID`: Consumer group ID (default: tracker-service)
- `KAFKA_PEER_CONNECTED_TOPIC`: Topic for peer connections (default: peer.connection.established)
- `KAFKA_PEER_DISCONNECTED_TOPIC`: Topic for peer disconnections (default: peer.connection.closed)
- `KAFKA_PEER_CATALOG_UPDATED_TOPIC`: Topic for catalog updates (default: peer.catalog.updated)
- `KAFKA_CATALOG_AVAILABILITY_CHANGED_TOPIC`: Topic for availability changes (default: catalog.availability.changed)
- `KAFKA_CATALOG_SUBSCRIPTION_MATCHED_TOPIC`: Topic for subscription matches (default: catalog.subscription.matched)

### Redis Configuration

- `REDIS_HOST`: Redis hostname (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `REDIS_PASSWORD`: Redis password (default: empty)

### Service Configuration

- `HEARTBEAT_INTERVAL_SECONDS`: Interval for stale peer checks (default: 30)
- `CONNECTION_TIMEOUT_SECONDS`: Peer connection timeout (default: 120)
- `SUBSCRIPTION_DEFAULT_TTL_HOURS`: Default subscription TTL (default: 24)

## Event Flow

### Consuming Events

- `peer.connection.established`: When a peer connects to an edge
- `peer.connection.closed`: When a peer disconnects
- `peer.catalog.updated`: When a peer's catalog changes

### Publishing Events

- `catalog.availability.changed`: When catalog availability changes
- `catalog.subscription.matched`: When subscribed catalog becomes available

## Development

```bash
# Run tests
poetry run pytest

# Run linting
poetry run flake8 src tests
```