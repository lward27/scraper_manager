"""
Async HTTP client for scraper-manager.

Provides retry logic with exponential backoff, circuit breaker integration,
and structured error handling for both yfinance wrapper and database service.
"""

import asyncio
import random
from datetime import date
from typing import Optional

import aiohttp
from aiohttp import ClientError, ClientResponseError

from scraper_manager.config import Config
from scraper_manager.logger import get_logger, with_context
from scraper_manager.metrics import metrics, Timer
from scraper_manager.circuit_breaker import (
    CircuitBreakerOpen,
    yfinance_circuit,
    database_circuit,
)

log = get_logger(__name__)


class HTTPClient:
    """Async HTTP client with retry logic and circuit breaker."""

    def __init__(self, config: Config):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.timeout.request_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("HTTPClient not initialized. Use async context manager.")
        return self._session

    async def _retry_with_backoff(
        self,
        url: str,
        params: Optional[dict] = None,
        method: str = "GET",
        json_body: Optional[dict] = None,
        timeout: Optional[int] = None,
    ) -> aiohttp.ClientResponse:
        """
        Make an HTTP request with exponential backoff retry.

        Retries on:
        - Connection errors
        - Timeouts
        - 429 (rate limit)
        - 5xx server errors

        Does NOT retry on 404 (legitimate not-found).
        """
        max_retries = self.config.retry.max_retries
        base_delay = self.config.retry.base_delay
        max_delay = self.config.retry.max_delay
        request_timeout = timeout or self.config.timeout.request_timeout

        last_exception: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                request_timeout_obj = aiohttp.ClientTimeout(total=request_timeout)

                if method == "GET":
                    async with self.session.get(
                        url, params=params, timeout=request_timeout_obj
                    ) as response:
                        # Don't retry 404
                        if response.status == 404:
                            return response
                        # Raise for 4xx/5xx, but we'll catch and retry 5xx/429
                        if response.status >= 500 or response.status == 429:
                            response.raise_for_status()
                        return response

                elif method == "POST":
                    async with self.session.post(
                        url, params=params, json=json_body, timeout=request_timeout_obj
                    ) as response:
                        if response.status >= 500 or response.status == 429:
                            response.raise_for_status()
                        return response

            except ClientResponseError as e:
                last_exception = e
                if e.status == 404:
                    # Don't retry 404
                    raise
                if e.status not in (429, 500, 502, 503, 504):
                    # Don't retry other 4xx errors
                    raise

            except (ClientError, asyncio.TimeoutError) as e:
                last_exception = e

            # Retry with backoff
            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                log.logger.warning(
                    f"Retry {attempt + 1}/{max_retries} for {url} after {delay:.1f}s "
                    f"(error: {last_exception})"
                )
                await asyncio.sleep(delay)

        # All retries exhausted
        if last_exception:
            raise last_exception
        raise RuntimeError(f"Unexpected: no exception but all retries exhausted for {url}")

    async def get_tickers_needing_update(self) -> list[dict]:
        """
        Fetch all tickers whose last price_history date is before yesterday.

        Returns list of dicts with keys: ticker_id, ticker, last_date
        """
        url = f"{self.config.services.database_service_url}/tickers/update-status"
        timeout = self.config.timeout.status_check_timeout

        log.logger.info(f"Fetching tickers needing update from {url}")

        try:
            response = await self._retry_with_backoff(url, timeout=timeout)
            response.raise_for_status()
            tickers = await response.json()

            # Filter to only tickers needing update (last_date < yesterday)
            yesterday = date.today()
            filtered = [
                t for t in tickers
                if date.fromisoformat(t["last_date"]) < yesterday
            ]

            log.logger.info(f"Found {len(filtered)} tickers needing update (out of {len(tickers)} total)")
            return filtered

        except Exception as e:
            log.logger.error(f"Failed to fetch tickers: {e}")
            raise

    async def fetch_yfinance(
        self,
        ticker: str,
        period: Optional[str] = None,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> Optional[dict]:
        """
        Fetch price history from yfinance wrapper.

        For new tickers: use period="max".
        For incremental: use start/end date range.

        Returns None on 404 (ticker not found or no data in range).
        """
        url = f"{self.config.services.yfinance_service_url}/history"
        params = {"ticker_name": ticker}

        if period:
            params["period"] = period
        if start:
            params["start"] = str(start)
        if end:
            params["end"] = str(end)

        ticker_log = with_context(log, ticker=ticker, operation="fetch")

        try:
            with Timer(metrics.histograms["scraper_fetch_duration_seconds"]):
                metrics.counters["scraper_api_calls_total"].inc()

                response = await yfinance_circuit.call(
                    self._retry_with_backoff, url, params=params
                )

            if response.status == 404:
                ticker_log.logger.info(f"No data found for {ticker}")
                return None

            response.raise_for_status()
            data = await response.json()
            return data

        except CircuitBreakerOpen as e:
            metrics.counters["scraper_circuit_breaker_trips"].inc()
            ticker_log.logger.warning(f"Circuit breaker open: {e}")
            raise
        except Exception as e:
            metrics.counters["scraper_api_errors_total"].inc()
            ticker_log.logger.error(f"Fetch failed for {ticker}: {e}")
            raise

    async def save_batch(self, rows: list[dict]) -> None:
        """
        POST a batch of OHLCV rows to the database service.

        Uses ON CONFLICT DO NOTHING for idempotency.
        Chunks large batches to avoid payload size limits.
        """
        if not rows:
            return

        batch_size = self.config.chunk.batch_size
        url = f"{self.config.services.database_service_url}/history/batch"

        # Split into chunks if necessary
        chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

        for i, chunk in enumerate(chunks):
            ticker_id = chunk[0].get("ticker_id", "unknown") if chunk else "unknown"
            ticker_log = with_context(log, ticker=str(ticker_id), operation="save")

            try:
                with Timer(metrics.histograms["scraper_save_duration_seconds"]):
                    metrics.counters["scraper_db_writes_total"].inc()

                    await database_circuit.call(
                        self._retry_with_backoff,
                        url,
                        method="POST",
                        json_body=chunk,
                        timeout=self.config.timeout.batch_timeout,
                    )

                ticker_log.logger.info(
                    f"Saved chunk {i + 1}/{len(chunks)} ({len(chunk)} rows)"
                )
                metrics.counters["scraper_rows_saved"].inc(len(chunk))

            except CircuitBreakerOpen as e:
                metrics.counters["scraper_circuit_breaker_trips"].inc()
                ticker_log.logger.warning(f"Circuit breaker open during save: {e}")
                raise
            except Exception as e:
                metrics.counters["scraper_db_errors_total"].inc()
                ticker_log.logger.error(f"Save failed for chunk {i + 1}: {e}")
                raise
