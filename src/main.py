import asyncio
import logging
import signal
from concurrent import futures

import grpc

from src.config import config
from src.dependencies import get_tracker_service
from src.grpc.server import TrackerServicer
from src.grpc.generated.tracker_pb2_grpc import add_TrackerServiceServicer_to_server
from src.services.kafka_service import KafkaService

logger = logging.getLogger(__name__)


async def start_grpc_server():
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

    # Get service instance
    tracker_service = get_tracker_service()

    # Add servicer to server
    add_TrackerServiceServicer_to_server(
        TrackerServicer(tracker_service), server
    )

    # Add secure credentials if needed
    # if config.grpc.use_tls:
    #     credentials = ...
    #     server.add_secure_port(config.grpc.address, credentials)
    # else:
    server.add_insecure_port(config.grpc.address)

    await server.start()

    logger.info(f"gRPC server started on {config.grpc.address}")

    return server


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
    grpc_server = await start_grpc_server()
    kafka_service = await start_kafka_consumer()

    # Setup graceful shutdown
    async def shutdown(signal, loop):
        logger.info(f"Received exit signal {signal.name}...")

        await grpc_server.stop(5)
        kafka_service.stop()

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