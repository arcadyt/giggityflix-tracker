import asyncio
import logging
import signal

from giggityflix_tracker.services.kafka_service import KafkaService

logger = logging.getLogger(__name__)


async def start_kafka_consumer():
    # Start Kafka consumer
    kafka_service = KafkaService()

    logger.info("Kafka consumer started")

    return kafka_service


async def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Start services
    kafka_service = await start_kafka_consumer()

    # Setup graceful shutdown
    async def shutdown(signal, loop):
        logger.info(f"Received exit signal {signal.name}...")

        await kafka_service.stop()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]

        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    # Register signals
    loop = asyncio.get_event_loop()
    for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop))
        )

    # Keep application running
    try:
        await asyncio.Event().wait()
    finally:
        loop.close()


if __name__ == "__main__":
    asyncio.run(main())
