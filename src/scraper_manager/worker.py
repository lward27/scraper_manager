"""Worker service that consumes ticker tasks from RabbitMQ."""

import asyncio
from datetime import date, datetime, timedelta, timezone

from aio_pika.abc import AbstractIncomingMessage

from scraper_manager.config import Config
from scraper_manager.http_client import HTTPClient
from scraper_manager.logger import get_logger, with_context
from scraper_manager.messages import TickerTaskMessage
from scraper_manager.metrics import metrics
from scraper_manager.rabbitmq_client import RabbitMQClient
from scraper_manager.transform import transform_chunk

log = get_logger(__name__)


class WorkerService:
    def __init__(self, config: Config, client: HTTPClient, rabbitmq: RabbitMQClient):
        self.config = config
        self.client = client
        self.rabbitmq = rabbitmq
        self.consumer_tag: str | None = None

    def _retry_delay_seconds(self, attempt: int) -> float:
        base = self.config.worker.retry_base_delay_seconds
        max_delay = self.config.worker.retry_max_delay_seconds
        return min(base * (2 ** max(0, attempt)), max_delay)

    async def _process_ticker(self, task: TickerTaskMessage) -> int:
        ticker_log = with_context(log, ticker=task.ticker, run_id=task.run_id, operation="process")

        last_date = date.fromisoformat(task.last_date)
        target_date = date.fromisoformat(task.target_date)

        if target_date < last_date:
            ticker_log.logger.info("Ticker already current; skipping")
            return 0

        if last_date <= date(1900, 1, 2):
            raw = await self.client.fetch_yfinance(task.ticker, period="max")
            if raw is None:
                return 0
            rows = transform_chunk(raw, task.ticker_id)
            if rows and not self.config.runtime.shadow_mode:
                await self.client.save_batch(rows)
            return len(rows)

        rows_saved = 0
        current_start = last_date + timedelta(days=1)

        while current_start <= target_date:
            chunk_end = min(current_start + timedelta(days=self.config.worker.chunk_days), target_date)
            raw = await self.client.fetch_yfinance(
                task.ticker,
                start=current_start,
                end=chunk_end + timedelta(days=1),
            )

            if raw is not None:
                rows = transform_chunk(raw, task.ticker_id)
                if rows and not self.config.runtime.shadow_mode:
                    await self.client.save_batch(rows)
                rows_saved += len(rows)

            current_start = chunk_end + timedelta(days=1)

        return rows_saved

    async def _record_progress(
        self,
        run_id: str,
        queued_delta: int = 0,
        processed_delta: int = 0,
        failed_delta: int = 0,
        dlq_delta: int = 0,
    ) -> None:
        try:
            await self.client.update_scraper_run_progress(
                run_id=run_id,
                queued_delta=queued_delta,
                processed_delta=processed_delta,
                failed_delta=failed_delta,
                dlq_delta=dlq_delta,
            )
        except Exception as exc:
            log.logger.warning("Failed to publish run progress for %s: %s", run_id, exc)

    async def handle_message(self, message: AbstractIncomingMessage) -> None:
        try:
            task = TickerTaskMessage.from_bytes(message.body)
        except Exception as exc:
            metrics.counters["scraper_queue_invalid_messages_total"].inc()
            log.logger.error("Rejecting invalid message: %s", exc)
            await message.reject(requeue=False)
            return

        now_utc = datetime.now(timezone.utc)
        try:
            enqueued_at = datetime.fromisoformat(task.enqueued_at)
            lag_seconds = max(0.0, (now_utc - enqueued_at).total_seconds())
            metrics.histograms["scraper_queue_lag_seconds"].observe(lag_seconds)
        except ValueError:
            pass

        metrics.counters["scraper_queue_consumed_total"].inc()
        metrics.gauges["scraper_active_workers"].inc()

        try:
            rows_saved = await self._process_ticker(task)
            metrics.counters["scraper_tickers_success"].inc()
            metrics.counters["scraper_rows_saved"].inc(rows_saved)
            metrics.gauges["scraper_queue_depth"].dec()
            await self._record_progress(run_id=task.run_id, processed_delta=1)
            await message.ack()
        except Exception as exc:
            metrics.counters["scraper_tickers_failed"].inc()
            try:
                if task.attempt < self.config.worker.max_retries:
                    next_task = task.next_attempt()
                    delay_seconds = self._retry_delay_seconds(task.attempt)
                    await self.rabbitmq.publish_retry(next_task.to_dict(), delay_seconds, str(exc))
                    metrics.counters["scraper_queue_retried_total"].inc()
                    log.logger.warning(
                        "Retry scheduled for %s attempt=%s delay=%.1fs error=%s",
                        task.ticker,
                        next_task.attempt,
                        delay_seconds,
                        exc,
                    )
                else:
                    await self.rabbitmq.publish_dlq(task.to_dict(), str(exc))
                    metrics.counters["scraper_queue_dlq_total"].inc()
                    await self._record_progress(run_id=task.run_id, failed_delta=1, dlq_delta=1)
                    log.logger.error("DLQ published for %s after retries: %s", task.ticker, exc)
                await message.ack()
            except Exception as publish_exc:
                log.logger.error(
                    "Failed to route failed message for %s (attempt=%s), requeueing original: %s",
                    task.ticker,
                    task.attempt,
                    publish_exc,
                )
                await message.nack(requeue=True)
        finally:
            metrics.gauges["scraper_active_workers"].dec()

    async def run(self, shutdown_event: asyncio.Event) -> None:
        self.consumer_tag = await self.rabbitmq.consume(self.handle_message)
        log.logger.info("Worker consumer started: %s", self.consumer_tag)
        await shutdown_event.wait()

        if self.consumer_tag:
            await self.rabbitmq.cancel_consumer(self.consumer_tag)
