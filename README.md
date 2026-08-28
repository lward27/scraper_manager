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
- `RABBITMQ_PREFETCH_COUNT` — must be a positive integer
- `LOG_LEVEL`
- `LOG_FORMAT`
- `HEALTH_PORT`

### Scheduler

- `SCHEDULER_TIMEZONE` (default: `America/New_York`)
- `SCHEDULER_RUN_HOUR` (default: `19`) — must be between 0 and 23 inclusive
- `SCHEDULER_RUN_MINUTE` (default: `0`) — must be between 0 and 59 inclusive
- `SCHEDULER_WEEKDAYS` (default: `0,1,2,3,4`)
- `SCHEDULER_POLL_SECONDS` (default: `30`)
- `SCHEDULER_PAGE_SIZE` (default: `500`) — must be a positive integer

### Worker

- `CHUNK_DAYS`
- `BATCH_SIZE`
- `REQUEST_TIMEOUT`
- `BATCH_TIMEOUT`
- `STATUS_CHECK_TIMEOUT`
- `WORKER_MAX_RETRIES` — must be a positive integer (zero and negative values are rejected with the stable error: `WORKER_MAX_RETRIES must be a positive integer`)
- `WORKER_RETRY_BASE_DELAY_SECONDS`
- `WORKER_RETRY_MAX_DELAY_SECONDS`

### Concurrency

- `MAX_CONCURRENT_YFINANCE_CALLS` (default: `4`) — must be a positive integer
- `MAX_CONCURRENT_DB_CALLS` (default: `4`) — must be a positive integer
- `MAX_WORKERS` (default: `8`) — must be a positive integer

## Startup Validation

All configuration values are validated immediately after `Config.from_env()` returns and before any downstream service (logger, health server, HTTP client, RabbitMQ, scheduler, or worker) is started. Validation is pure, deterministic, and side-effect free. The first violation encountered raises a `ConfigValidationError` with a stable field-specific message, causing a clean early exit with the error logged to stderr.

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
