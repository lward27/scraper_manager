# Scraper Manager v3.0

Queue-driven stock data pipeline for the Lucas Engineering finance stack.

## Runtime Model

`scraper_manager` now runs as long-lived services instead of a `CronJob`/`Job`:

- **Scheduler mode** (`MODE=scheduler`)
  - Runs continuously.
  - At 7:00 PM ET on weekdays, enqueues stale ticker tasks to RabbitMQ.
  - Uses `finance_app_database_service` for stale ticker discovery and run tracking.
- **Worker mode** (`MODE=worker`)
  - Consumes ticker tasks from RabbitMQ.
  - Fetches prices via `yfinance_wrapper`.
  - Persists data via `finance_app_database_service`.
  - Uses bounded retries with exponential backoff and dead-letter queue fallback.

## Queues

- `scraper.ticker.work`
- `scraper.ticker.retry`
- `scraper.ticker.dlq`

## Environment Variables

### Common

- `MODE`: `scheduler` or `worker`
- `SHADOW_MODE`: `true|false` (when `true`, workers fetch/transform but skip DB writes)
- `DATABASE_SERVICE_URL`
- `YFINANCE_SERVICE_URL`
- `RABBITMQ_URL`
- `RABBITMQ_WORK_QUEUE`
- `RABBITMQ_RETRY_QUEUE`
- `RABBITMQ_DLQ_QUEUE`
- `RABBITMQ_PREFETCH_COUNT`
- `LOG_LEVEL`
- `LOG_FORMAT`
- `HEALTH_PORT`

### Scheduler

- `SCHEDULER_TIMEZONE` (default: `America/New_York`)
- `SCHEDULER_RUN_HOUR` (default: `19`)
- `SCHEDULER_RUN_MINUTE` (default: `0`)
- `SCHEDULER_WEEKDAYS` (default: `0,1,2,3,4`)
- `SCHEDULER_POLL_SECONDS` (default: `30`)
- `SCHEDULER_PAGE_SIZE` (default: `500`)

### Worker

- `CHUNK_DAYS`
- `BATCH_SIZE`
- `REQUEST_TIMEOUT`
- `BATCH_TIMEOUT`
- `STATUS_CHECK_TIMEOUT`
- `WORKER_MAX_RETRIES`
- `WORKER_RETRY_BASE_DELAY_SECONDS`
- `WORKER_RETRY_MAX_DELAY_SECONDS`

## Local Run

```bash
pip install -r requirements.txt
python -m scraper_manager
```

Set `MODE` explicitly before starting.

## Tests

```bash
pytest tests -v
```
