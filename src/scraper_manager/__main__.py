"""
Entry point for scraper-manager.

Can be run as:
    python -m scraper_manager

Or with custom config:
    LOG_LEVEL=DEBUG MAX_WORKERS=4 python -m scraper_manager
"""

import asyncio
import sys
import signal
import os

from scraper_manager.config import Config
from scraper_manager.logger import get_logger
from scraper_manager.orchestrator import run_scraper
from scraper_manager.metrics import metrics
from scraper_manager.health_server import start_health_server, stop_health_server, set_ready

log = get_logger(__name__)


def main():
    """Main entry point with signal handling, health server, and graceful shutdown."""
    config = Config.from_env()

    # Configure logging
    from scraper_manager.logger import get_logger as _get_logger
    global log
    log = _get_logger(
        level=config.logging.log_level,
        json_format=(config.logging.log_format == "json"),
    )

    # Start health check server
    health_port = int(os.getenv("HEALTH_PORT", "8080"))
    health_thread = start_health_server(port=health_port)

    # Handle SIGTERM/SIGINT for graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    shutdown_event = asyncio.Event()

    def handle_signal(sig):
        log.logger.info(f"Received signal {sig.name}, shutting down gracefully...")
        set_ready(False)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal, sig)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    async def run_with_shutdown():
        set_ready(True)
        try:
            result = await run_scraper(config)

            # Print metrics summary
            log.logger.info("=== Metrics Summary ===")
            log.logger.info(f"Tickers processed: {result['processed']}")
            log.logger.info(f"Tickers failed: {result['failed']}")
            log.logger.info(f"Rows saved: {result['rows_saved']}")

            if result.get("errors"):
                log.logger.warning(f"Errors: {len(result['errors'])}")

            set_ready(False)

            # Exit with error code if any failures
            if result["failed"] > 0:
                sys.exit(1)
            sys.exit(0)

        except Exception as e:
            log.logger.error(f"Fatal error: {e}", exc_info=True)
            set_ready(False)
            sys.exit(1)

    try:
        loop.run_until_complete(run_with_shutdown())
    except KeyboardInterrupt:
        log.logger.info("Interrupted by user")
        set_ready(False)
        sys.exit(130)
    finally:
        stop_health_server()
        loop.close()


if __name__ == "__main__":
    main()
