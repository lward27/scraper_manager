"""
Main orchestrator for scraper-manager v2.0.

Coordinates fetching and storing stock market data with:
- Async/await concurrency
- Circuit breaker protection
- Structured logging and metrics
- Incremental updates with smart chunking
- Backpressure via semaphores
"""

import asyncio
import time
from datetime import date, timedelta
from typing import Optional

from scraper_manager.config import Config
from scraper_manager.logger import get_logger, with_context
from scraper_manager.metrics import metrics, Timer
from scraper_manager.http_client import HTTPClient
from scraper_manager.transform import transform_chunk
from scraper_manager.circuit_breaker import CircuitBreakerOpen

log = get_logger(__name__)


class TickerProcessor:
    """
    Processes a single ticker: fetches missing data and saves to database.

    New tickers (no prior data): single period=max call.
    Existing tickers: incremental chunks from last_date+1 to yesterday.
    """

    def __init__(
        self,
        client: HTTPClient,
        config: Config,
        yfinance_semaphore: asyncio.Semaphore,
        db_semaphore: asyncio.Semaphore,
    ):
        self.client = client
        self.config = config
        self.yfinance_semaphore = yfinance_semaphore
        self.db_semaphore = db_semaphore

    async def process(self, ticker_info: dict) -> tuple[str, int, Optional[str]]:
        """
        Fetch and store all missing price history for one ticker.

        Returns:
            (ticker_symbol, rows_saved, error_message_or_None)
        """
        ticker_id = ticker_info["ticker_id"]
        ticker_sym = ticker_info["ticker"]
        last_date = date.fromisoformat(ticker_info["last_date"])
        yesterday = date.today() - timedelta(days=1)

        ticker_log = with_context(log, ticker=ticker_sym, operation="process")
        start_time = time.monotonic()

        try:
            is_new = last_date <= date(1900, 1, 2)

            if is_new:
                rows_saved = await self._fetch_new_ticker(ticker_sym, ticker_id, ticker_log)
            else:
                rows_saved = await self._fetch_incremental(
                    ticker_sym, ticker_id, last_date, yesterday, ticker_log
                )

            elapsed = time.monotonic() - start_time
            metrics.histograms["scraper_ticker_duration_seconds"].observe(elapsed)
            metrics.counters["scraper_tickers_success"].inc()

            ticker_log.logger.info(
                f"Completed: {rows_saved} rows saved in {elapsed:.2f}s"
            )
            return ticker_sym, rows_saved, None

        except CircuitBreakerOpen as e:
            elapsed = time.monotonic() - start_time
            metrics.histograms["scraper_ticker_duration_seconds"].observe(elapsed)
            metrics.counters["scraper_tickers_failed"].inc()
            error_msg = f"circuit breaker open: {e}"
            ticker_log.logger.error(error_msg)
            return ticker_sym, 0, error_msg

        except Exception as e:
            elapsed = time.monotonic() - start_time
            metrics.histograms["scraper_ticker_duration_seconds"].observe(elapsed)
            metrics.counters["scraper_tickers_failed"].inc()
            error_msg = f"error: {e}"
            ticker_log.logger.error(error_msg)
            return ticker_sym, 0, error_msg

    async def _fetch_new_ticker(
        self,
        ticker: str,
        ticker_id: int,
        ticker_log,
    ) -> int:
        """Fetch full history for a new ticker using period=max."""
        async with self.yfinance_semaphore:
            raw = await self.client.fetch_yfinance(ticker, period="max")

        if raw is None:
            ticker_log.logger.warning("Ticker not found in yfinance (period=max)")
            return 0

        rows = transform_chunk(raw, ticker_id)
        if not rows:
            ticker_log.logger.warning("No rows transformed from yfinance response")
            return 0

        async with self.db_semaphore:
            await self.client.save_batch(rows)

        return len(rows)

    async def _fetch_incremental(
        self,
        ticker: str,
        ticker_id: int,
        start: date,
        end: date,
        ticker_log,
    ) -> int:
        """Fetch incremental chunks from start to end date."""
        chunk_days = self.config.chunk.chunk_days
        rows_saved = 0

        current_start = start
        chunk_num = 0

        while current_start <= end:
            chunk_end = min(current_start + timedelta(days=chunk_days), end)
            chunk_num += 1

            async with self.yfinance_semaphore:
                raw = await self.client.fetch_yfinance(
                    ticker, start=current_start, end=chunk_end
                )

            if raw is not None:
                rows = transform_chunk(raw, ticker_id)
                if rows:
                    async with self.db_semaphore:
                        await self.client.save_batch(rows)
                    rows_saved += len(rows)
                    ticker_log.logger.debug(
                        f"Chunk {chunk_num} ({current_start} to {chunk_end}): "
                        f"{len(rows)} rows"
                    )
                else:
                    ticker_log.logger.debug(
                        f"Chunk {chunk_num} ({current_start} to {chunk_end}): no data"
                    )
            else:
                ticker_log.logger.debug(
                    f"Chunk {chunk_num} ({current_start} to {chunk_end}): 404 (no data)"
                )

            current_start = chunk_end + timedelta(days=1)

        return rows_saved


async def run_scraper(config: Optional[Config] = None) -> dict:
    """
    Main entry point: fetch all tickers needing update and process them.

    Args:
        config: Optional config override. Uses Config.from_env() if not provided.

    Returns:
        Summary dict with counts of processed/failed tickers and rows saved.
    """
    if config is None:
        config = Config.from_env()

    ticker_log = with_context(log, operation="run")
    ticker_log.logger.info("Scraper Manager v2.0 starting")

    # Create HTTP client
    async with HTTPClient(config) as client:
        # Fetch tickers needing update
        try:
            tickers = await client.get_tickers_needing_update()
        except Exception as e:
            ticker_log.logger.error(f"Fatal: could not fetch tickers - {e}")
            raise

        if not tickers:
            ticker_log.logger.info("No tickers need updating. Done.")
            return {"processed": 0, "failed": 0, "rows_saved": 0}

        ticker_log.logger.info(f"Processing {len(tickers)} tickers")
        metrics.gauges["scraper_queue_depth"].set(len(tickers))

        # Create semaphores for backpressure
        yfinance_semaphore = asyncio.Semaphore(
            config.concurrency.max_concurrent_yfinance_calls
        )
        db_semaphore = asyncio.Semaphore(
            config.concurrency.max_concurrent_db_calls
        )

        # Create processor
        processor = TickerProcessor(
            client=client,
            config=config,
            yfinance_semaphore=yfinance_semaphore,
            db_semaphore=db_semaphore,
        )

        # Process all tickers concurrently with worker limit
        max_workers = config.concurrency.max_workers
        semaphore = asyncio.Semaphore(max_workers)

        async def process_with_semaphore(ticker_info: dict):
            async with semaphore:
                metrics.gauges["scraper_active_workers"].inc()
                try:
                    return await processor.process(ticker_info)
                finally:
                    metrics.gauges["scraper_active_workers"].dec()

        tasks = [process_with_semaphore(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        processed = 0
        failed = 0
        total_rows = 0
        errors = []

        for result in results:
            if isinstance(result, Exception):
                failed += 1
                errors.append(f"Unexpected error: {result}")
            else:
                sym, rows, error = result
                processed += 1
                total_rows += rows
                if error:
                    failed += 1
                    errors.append(f"{sym}: {error}")

        # Final summary
        ticker_log.logger.info(
            f"Scraper Manager complete: "
            f"{processed} tickers, {failed} failed, {total_rows} rows saved"
        )

        if errors:
            ticker_log.logger.warning(f"Errors encountered ({len(errors)}):")
            for err in errors[:10]:  # Log first 10 errors
                ticker_log.logger.warning(f"  {err}")
            if len(errors) > 10:
                ticker_log.logger.warning(f"  ... and {len(errors) - 10} more")

        return {
            "processed": processed,
            "failed": failed,
            "rows_saved": total_rows,
            "errors": errors,
        }
