"""Async HTTP client for scheduler and worker operations."""

import asyncio
import random
from datetime import date
from typing import Optional

import aiohttp
from aiohttp import ClientError, ClientResponseError

from scraper_manager.config import Config
from scraper_manager.logger import get_logger, with_context
from scraper_manager.metrics import metrics, Timer

log = get_logger(__name__)


class HTTPClient:
    def __init__(self, config: Config):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.worker.request_timeout)
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
        max_retries = self.config.worker.max_retries
        base_delay = self.config.worker.retry_base_delay_seconds
        max_delay = self.config.worker.retry_max_delay_seconds
        request_timeout = timeout or self.config.worker.request_timeout

        last_exception: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                request_timeout_obj = aiohttp.ClientTimeout(total=request_timeout)

                if method == "GET":
                    response = await self.session.get(
                        url,
                        params=params,
                        timeout=request_timeout_obj,
                    )
                    if response.status == 404:
                        return response
                    if response.status >= 500 or response.status == 429:
                        status = response.status
                        message = await response.text()
                        request_info = response.request_info
                        history = response.history
                        headers = response.headers
                        response.release()
                        raise ClientResponseError(
                            request_info=request_info,
                            history=history,
                            status=status,
                            message=message,
                            headers=headers,
                        )
                    return response

                if method == "POST":
                    response = await self.session.post(
                        url,
                        params=params,
                        json=json_body,
                        timeout=request_timeout_obj,
                    )
                    if response.status >= 500 or response.status == 429:
                        status = response.status
                        message = await response.text()
                        request_info = response.request_info
                        history = response.history
                        headers = response.headers
                        response.release()
                        raise ClientResponseError(
                            request_info=request_info,
                            history=history,
                            status=status,
                            message=message,
                            headers=headers,
                        )
                    return response

                raise ValueError(f"Unsupported HTTP method: {method}")

            except ClientResponseError as exc:
                last_exception = exc
                if exc.status == 404:
                    raise
                if exc.status not in (429, 500, 502, 503, 504):
                    raise
            except (ClientError, asyncio.TimeoutError) as exc:
                last_exception = exc

            if attempt < max_retries - 1:
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                log.logger.warning(
                    "Retry %s/%s for %s after %.1fs (error: %s)",
                    attempt + 1,
                    max_retries,
                    url,
                    delay,
                    last_exception,
                )
                await asyncio.sleep(delay)

        if last_exception:
            raise last_exception
        raise RuntimeError(f"No response and no exception for {url}")

    async def get_stale_tickers(
        self,
        target_date: date,
        offset: int,
        limit: int,
    ) -> list[dict]:
        url = f"{self.config.services.database_service_url}/tickers/stale"
        response = await self._retry_with_backoff(
            url,
            params={
                "target_date": str(target_date),
                "offset": offset,
                "limit": limit,
            },
            timeout=self.config.worker.status_check_timeout,
        )
        try:
            response.raise_for_status()
            return await response.json()
        finally:
            response.release()

    async def start_scraper_run(
        self,
        run_key: str,
        scheduled_for: date,
        shadow_mode: bool,
        mode: str = "daily",
    ) -> dict:
        url = f"{self.config.services.database_service_url}/scraper-runs/start"
        response = await self._retry_with_backoff(
            url,
            method="POST",
            json_body={
                "run_key": run_key,
                "scheduled_for": str(scheduled_for),
                "mode": mode,
                "shadow_mode": shadow_mode,
            },
        )
        try:
            response.raise_for_status()
            return await response.json()
        finally:
            response.release()

    async def update_scraper_run_progress(
        self,
        run_id: str,
        queued_delta: int = 0,
        processed_delta: int = 0,
        failed_delta: int = 0,
        dlq_delta: int = 0,
    ) -> None:
        url = f"{self.config.services.database_service_url}/scraper-runs/{run_id}/progress"
        response = await self._retry_with_backoff(
            url,
            method="POST",
            json_body={
                "queued_delta": queued_delta,
                "processed_delta": processed_delta,
                "failed_delta": failed_delta,
                "dlq_delta": dlq_delta,
            },
        )
        response.raise_for_status()
        response.release()

    async def complete_scraper_run(self, run_id: str, status: str) -> None:
        url = f"{self.config.services.database_service_url}/scraper-runs/{run_id}/complete"
        response = await self._retry_with_backoff(
            url,
            method="POST",
            json_body={"status": status},
        )
        response.raise_for_status()
        response.release()

    async def fetch_yfinance(
        self,
        ticker: str,
        period: Optional[str] = None,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> Optional[dict]:
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
                response = await self._retry_with_backoff(url, params=params)

            if response.status == 404:
                ticker_log.logger.info("No yfinance data found")
                response.release()
                return None

            try:
                response.raise_for_status()
                return await response.json()
            finally:
                response.release()
        except Exception as exc:
            metrics.counters["scraper_api_errors_total"].inc()
            ticker_log.logger.error("Fetch failed: %s", exc)
            raise

    async def save_batch(self, rows: list[dict]) -> None:
        if not rows:
            return

        batch_size = self.config.worker.batch_size
        url = f"{self.config.services.database_service_url}/history/batch"

        chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

        for chunk in chunks:
            try:
                with Timer(metrics.histograms["scraper_save_duration_seconds"]):
                    metrics.counters["scraper_db_writes_total"].inc()
                    response = await self._retry_with_backoff(
                        url,
                        method="POST",
                        json_body=chunk,
                        timeout=self.config.worker.batch_timeout,
                    )
                    response.raise_for_status()
                    response.release()
                metrics.counters["scraper_rows_saved"].inc(len(chunk))
            except Exception:
                metrics.counters["scraper_db_errors_total"].inc()
                raise
