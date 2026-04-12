"""Scheduler service that enqueues stale ticker tasks on a fixed weekday schedule."""

import asyncio
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from scraper_manager.config import Config
from scraper_manager.http_client import HTTPClient
from scraper_manager.logger import get_logger, with_context
from scraper_manager.messages import TickerTaskMessage
from scraper_manager.metrics import metrics
from scraper_manager.rabbitmq_client import RabbitMQClient

log = get_logger(__name__)


class SchedulerService:
    def __init__(self, config: Config, client: HTTPClient, rabbitmq: RabbitMQClient):
        self.config = config
        self.client = client
        self.rabbitmq = rabbitmq
        self._last_triggered_date: date | None = None
        self._tz = ZoneInfo(self.config.scheduler.timezone)

    def _scheduled_time_for_date(self, day: date) -> datetime:
        return datetime.combine(
            day,
            time(hour=self.config.scheduler.run_hour, minute=self.config.scheduler.run_minute),
            tzinfo=self._tz,
        )

    def _should_trigger(self, now_local: datetime) -> bool:
        if now_local.weekday() not in self.config.scheduler.weekdays:
            return False
        if self._last_triggered_date == now_local.date():
            return False
        return now_local >= self._scheduled_time_for_date(now_local.date())

    async def _enqueue_once(self, scheduled_day: date) -> None:
        target_date = scheduled_day + timedelta(days=self.config.scheduler.target_date_offset_days)
        run_key = f"daily-{target_date.isoformat()}"

        run = await self.client.start_scraper_run(
            run_key=run_key,
            scheduled_for=scheduled_day,
            shadow_mode=self.config.runtime.shadow_mode,
            mode="daily",
        )

        run_id = run["id"]
        existing_status = run.get("status", "")
        existing_queued = int(run.get("queued", 0) or 0)
        if existing_status in {"queued", "completed"} and existing_queued > 0:
            log.logger.info(
                "Run %s already initialized with status=%s queued=%s, skipping duplicate enqueue.",
                run_key,
                existing_status,
                existing_queued,
            )
            return

        total_queued = 0
        offset = 0
        page_size = self.config.scheduler.page_size

        scheduler_log = with_context(log, operation="enqueue", run_id=run_id)
        scheduler_log.logger.info(
            "Starting enqueue cycle for scheduled_day=%s target_date=%s",
            scheduled_day,
            target_date,
        )

        while True:
            page = await self.client.get_stale_tickers(
                target_date=target_date,
                offset=offset,
                limit=page_size,
            )
            if not page:
                break

            for ticker_info in page:
                task = TickerTaskMessage(
                    run_id=run_id,
                    ticker_id=ticker_info["ticker_id"],
                    ticker=ticker_info["ticker"],
                    last_date=ticker_info["last_date"],
                    target_date=str(target_date),
                    scheduled_for=str(scheduled_day),
                )
                await self.rabbitmq.publish_task(task.to_dict())

            page_count = len(page)
            total_queued += page_count
            offset += page_count
            metrics.counters["scraper_queue_published_total"].inc(page_count)
            metrics.gauges["scraper_queue_depth"].inc(page_count)
            await self.client.update_scraper_run_progress(run_id=run_id, queued_delta=page_count)

            if page_count < page_size:
                break

        status = "queued" if total_queued > 0 else "completed"
        await self.client.complete_scraper_run(run_id, status)
        scheduler_log.logger.info("Enqueue cycle complete: queued=%s status=%s", total_queued, status)

    async def run(self, shutdown_event: asyncio.Event) -> None:
        poll_seconds = self.config.scheduler.poll_interval_seconds

        while not shutdown_event.is_set():
            now_local = datetime.now(self._tz)
            if self._should_trigger(now_local):
                try:
                    await self._enqueue_once(now_local.date())
                    self._last_triggered_date = now_local.date()
                except Exception as exc:
                    log.logger.error("Scheduler enqueue failed: %s", exc, exc_info=True)

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                continue
