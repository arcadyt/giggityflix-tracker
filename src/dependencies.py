import aioredis
from redis import Redis

from src.config import config
from src.services.kafka_service import KafkaService
from src.services.redis_service import RedisService
from src.services.subscription_service import SubscriptionService
from src.services.tracker_service import TrackerService


async def get_redis_async():
    """Get async Redis client."""
    redis_config = config.redis
    return await aioredis.from_url(
        f"redis://{redis_config.host}:{redis_config.port}/{redis_config.db}",
        password=redis_config.password or None,
        encoding="utf-8",
        decode_responses=True
    )


def get_redis_sync():
    """Get synchronous Redis client."""
    redis_config = config.redis
    return Redis(
        host=redis_config.host,
        port=redis_config.port,
        db=redis_config.db,
        password=redis_config.password or None,
        decode_responses=True
    )


def get_redis_service():
    """Get RedisService instance."""
    redis_sync = get_redis_sync()
    return RedisService(redis_sync)


def get_kafka_service():
    """Get KafkaService instance."""
    return KafkaService()


def get_subscription_service():
    """Get SubscriptionService instance."""
    redis_service = get_redis_service()
    kafka_service = get_kafka_service()
    return SubscriptionService(redis_service, kafka_service)


def get_tracker_service():
    """Get TrackerService instance."""
    redis_service = get_redis_service()
    kafka_service = get_kafka_service()
    subscription_service = get_subscription_service()
    return TrackerService(redis_service, kafka_service, subscription_service)
