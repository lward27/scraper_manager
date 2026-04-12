from datetime import date

import pytest

from scraper_manager.config import Config
from scraper_manager.scheduler import SchedulerService


class DummyClient:
    def __init__(self):
        self.started = []
        self.progress = []
        self.completed = []
        self.pages = [
            [
                {"ticker_id": 1, "ticker": "AAPL", "last_date": "2026-04-09"},
                {"ticker_id": 2, "ticker": "MSFT", "last_date": "2026-04-08"},
            ],
            [],
        ]

    async def start_scraper_run(self, run_key, scheduled_for, shadow_mode, mode):
        self.started.append((run_key, scheduled_for, shadow_mode, mode))
        return {"id": "run-123"}

    async def get_stale_tickers(self, target_date, offset, limit):
        return self.pages.pop(0)

    async def update_scraper_run_progress(self, run_id, queued_delta=0, processed_delta=0, failed_delta=0, dlq_delta=0):
        self.progress.append((run_id, queued_delta, processed_delta, failed_delta, dlq_delta))

    async def complete_scraper_run(self, run_id, status):
        self.completed.append((run_id, status))


class DummyRabbit:
    def __init__(self):
        self.published = []

    async def publish_task(self, payload):
        self.published.append(payload)


@pytest.mark.asyncio
async def test_scheduler_enqueues_stale_tickers():
    config = Config.from_env()
    config.runtime.shadow_mode = True

    client = DummyClient()
    rabbit = DummyRabbit()
    scheduler = SchedulerService(config=config, client=client, rabbitmq=rabbit)

    await scheduler._enqueue_once(date(2026, 4, 10))

    assert len(rabbit.published) == 2
    assert client.started[0][0] == "daily-2026-04-10"
    assert client.completed == [("run-123", "queued")]
