"""Runtime entrypoint for queue-based scraper manager."""

import asyncio
import signal
import sys

from scraper_manager.config import Config
from scraper_manager.health_server import start_health_server, stop_health_server, set_ready
from scraper_manager.http_client import HTTPClient
from scraper_manager.logger import get_logger
from scraper_manager.rabbitmq_client import RabbitMQClient
from scraper_manager.scheduler import SchedulerService
from scraper_manager.worker import WorkerService

log = get_logger(__name__)


async def _run_service(config: Config, shutdown_event: asyncio.Event) -> None:
    rabbitmq = RabbitMQClient(config)

    async with HTTPClient(config) as client:
        await rabbitmq.connect()

        if config.runtime.mode == "scheduler":
            service = SchedulerService(config=config, client=client, rabbitmq=rabbitmq)
            log.logger.info("Starting scraper-manager scheduler mode")
        elif config.runtime.mode == "worker":
            service = WorkerService(config=config, client=client, rabbitmq=rabbitmq)
            log.logger.info("Starting scraper-manager worker mode")
        else:
            raise ValueError(f"Unsupported MODE: {config.runtime.mode}")

        set_ready(True)
        try:
            await service.run(shutdown_event)
        finally:
            set_ready(False)
            await rabbitmq.close()


def main() -> None:
    config = Config.from_env()

    global log
    log = get_logger(
        "scraper_manager",
        level=config.logging.log_level,
        json_format=(config.logging.log_format == "json"),
    )

    start_health_server(port=config.runtime.health_port)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown_event = asyncio.Event()

    def _handle_signal(sig):
        log.logger.info("Received signal %s, shutting down", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(_run_service(config, shutdown_event))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        log.logger.error("Fatal service error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        stop_health_server()
        loop.close()


if __name__ == "__main__":
    main()
