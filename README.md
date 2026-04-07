# Scraper Manager v2.0

Production-grade stock market data scraper for the Lucas Engineering finance stack.

## Architecture

### Design Decision: Async/Await (Option A)

We chose **Option A (Improved Python with Async/Await)** over event-driven (Option B) or workflow engine (Option C) architectures for the following reasons:

| Criterion | Option A (Async) | Option B (Event-Driven) | Option C (Workflow Engine) |
|-----------|-----------------|------------------------|---------------------------|
| **K8s Simplicity** | ✅ Single CronJob/Job, no extra infra | ❌ Requires Redis/NATS | ❌ Requires Temporal/Cadence |
| **Operational Overhead** | ✅ Minimal - just Python | ❌ Manage message queue | ❌ Manage workflow engine |
| **Reliability** | ✅ Circuit breaker + retries | ✅ Queue provides durability | ✅ Built-in retry/orchestration |
| **Observability** | ✅ Structured logging + metrics | ✅ Plus queue metrics | ✅ Built-in tracing |
| **Maintainability** | ✅ Standard Python, easy to debug | ❌ Distributed system complexity | ❌ New paradigm to learn |
| **Resource Efficiency** | ✅ Non-blocking I/O, low memory | ⚠️ Queue overhead | ❌ Worker overhead |
| **Fit for Purpose** | ✅ Daily batch job | ⚠️ Overkill for batch | ❌ Overkill for batch |

**Key reasoning:** The scraper-manager is a **daily batch CronJob** that runs for ~10-30 minutes. It doesn't need the durability guarantees of a message queue or the orchestration complexity of a workflow engine. Async/await gives us:

- **Non-blocking I/O**: No wasted threads waiting on HTTP responses
- **Proper concurrency control**: `asyncio.Semaphore` for backpressure
- **Clean error handling**: Circuit breaker prevents cascading failures
- **Simple deployment**: Single container, no external dependencies

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Scraper Manager Job                       │
│                                                              │
│  ┌──────────────┐    ┌────────────────┐    ┌──────────────┐ │
│  │  Orchestrator │───▶│  HTTP Client   │───▶│  Transform   │ │
│  │  (asyncio)    │    │  (aiohttp)     │    │  (pure fn)   │ │
│  └──────┬───────┘    └───────┬────────┘    └──────┬───────┘ │
│         │                    │                     │         │
│         │              ┌─────▼────────┐            │         │
│         │              │ Circuit      │            │         │
│         │              │ Breaker      │            │         │
│         │              └──────────────┘            │         │
│         │                                          │         │
│  ┌──────▼────────┐                                 │         │
│  │  Semaphores   │                                 │         │
│  │  (backpressure)│                                │         │
│  └───────────────┘                                 │         │
│                                                    │         │
│  ┌──────────────┐    ┌────────────────┐            │         │
│  │  Metrics     │◀───│  Logger        │◀───────────┘         │
│  │  (Prometheus)│    │  (JSON)        │                      │
│  └──────────────┘    └────────────────┘                      │
│                                                              │
│  ┌──────────────┐                                            │
│  │ Health Server │─── Port 8080 (/healthz, /ready, /metrics)│
│  └──────────────┘                                            │
└─────────────────────────────────────────────────────────────┘
         │                         │
         ▼                         ▼
┌─────────────────┐     ┌──────────────────────┐
│ yfinance-wrapper│     │ finance-app-db-service│
│ (port 8090)     │     │ (port 8091)           │
└─────────────────┘     └──────────────────────┘
```

### Key Components

1. **Orchestrator** (`orchestrator.py`): Main async loop that coordinates fetching and saving
2. **HTTP Client** (`http_client.py`): Async HTTP with retry logic and circuit breaker integration
3. **Circuit Breaker** (`circuit_breaker.py`): Prevents cascading failures when downstream services are unhealthy
4. **Transform** (`transform.py`): Pure function to convert yfinance response to database rows
5. **Metrics** (`metrics.py`): Prometheus-compatible metrics collection
6. **Logger** (`logger.py`): Structured JSON logging
7. **Health Server** (`health_server.py`): HTTP server for K8s probes and metrics endpoint

### Data Flow

1. **Fetch tickers needing update** from database service (`/tickers/update-status`)
2. **For each ticker:**
   - **New ticker** (no prior data): Fetch `period=max` from yfinance wrapper
   - **Existing ticker**: Fetch incremental chunks (90-day windows) from `last_date+1` to yesterday
3. **Transform** yfinance response to flat row format
4. **Save batch** to database service (`/history/batch`) with `ON CONFLICT DO NOTHING` for idempotency
5. **Backpressure**: Semaphore limits concurrent yfinance calls (4) and DB writes (4)
6. **Circuit breaker**: Trips after 10 consecutive failures, resets after 30s

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_SERVICE_URL` | `http://finance-app-database-service.apps-prod.svc.cluster.local:8091` | Database service endpoint |
| `YFINANCE_SERVICE_URL` | `http://yfinance-wrapper.apps-prod.svc.cluster.local:8090` | yfinance wrapper endpoint |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_FORMAT` | `json` | Log format (`json` or `text`) |
| `MAX_WORKERS` | `8` | Max concurrent ticker processing tasks |
| `MAX_CONCURRENT_YFINANCE_CALLS` | `4` | Max concurrent yfinance API calls |
| `MAX_CONCURRENT_DB_CALLS` | `4` | Max concurrent database writes |
| `CHUNK_DAYS` | `90` | Days per incremental fetch chunk |
| `BATCH_SIZE` | `500` | Rows per database batch |
| `MAX_RETRIES` | `5` | Max retry attempts for HTTP requests |
| `RETRY_BASE_DELAY` | `2.0` | Base delay for exponential backoff (seconds) |
| `RETRY_MAX_DELAY` | `60.0` | Max delay for exponential backoff (seconds) |
| `CIRCUIT_BREAKER_THRESHOLD` | `10` | Failures before circuit breaker trips |
| `CIRCUIT_BREAKER_RESET_TIMEOUT` | `30.0` | Seconds before circuit breaker resets |
| `REQUEST_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `BATCH_TIMEOUT` | `60` | Batch save timeout (seconds) |
| `STATUS_CHECK_TIMEOUT` | `300` | Ticker status check timeout (seconds) |
| `HEALTH_PORT` | `8080` | Health check server port |

## Deployment

### Build and Push Docker Image

```bash
cd /home/openclaw/.openclaw/workspace/scraper_manager

# Build for linux/amd64 (matching cluster architecture)
docker build --platform linux/amd64 -t registry.lucas.engineering/scraper_manager:v2.0 .

# Push to registry
docker push registry.lucas.engineering/scraper_manager:v2.0
```

### Deploy via ArgoCD

The Helm chart is in the `lucas_engineering` repo. After pushing the image:

```bash
cd /home/openclaw/.openclaw/workspace/lucas_engineering

# Commit the updated deployment
git add charts/scraper-manager/deployment.yaml
git commit -m "feat(scraper-manager): deploy v2.0 with async architecture"
git push origin main
```

ArgoCD will automatically sync the changes to the `apps-prod` namespace.

### Manual Deployment (for testing)

```bash
kubectl apply -f charts/scraper-manager/deployment.yaml -n apps-prod
```

### Run as CronJob (optional)

If you want to run this on a schedule instead of manually, create a CronJob wrapper:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: scraper-manager
spec:
  schedule: "0 6 * * *"  # Daily at 6 AM UTC
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: scraper-manager
            image: registry.lucas.engineering/scraper_manager:v2.0
            # ... (same env and resources as Job)
          restartPolicy: Never
```

## Observability

### Metrics

Scrape `/metrics` on port 8080 for Prometheus-compatible metrics:

- `scraper_tickers_total`: Total tickers processed
- `scraper_tickers_success`: Successfully processed tickers
- `scraper_tickers_failed`: Failed tickers
- `scraper_rows_saved`: Total rows saved to database
- `scraper_api_calls_total`: Total yfinance API calls
- `scraper_api_errors_total`: Total yfinance API errors
- `scraper_db_writes_total`: Total database write operations
- `scraper_db_errors_total`: Total database write errors
- `scraper_circuit_breaker_trips`: Circuit breaker trip count
- `scraper_active_workers`: Currently active worker tasks
- `scraper_queue_depth`: Tickers waiting to be processed
- `scraper_fetch_duration_seconds`: Yfinance fetch latency histogram
- `scraper_save_duration_seconds`: Database save latency histogram
- `scraper_ticker_duration_seconds`: Per-ticker processing latency histogram

### Logs

Structured JSON logs to stdout. Example:

```json
{
  "timestamp": "2026-04-07T15:30:00.000000+00:00",
  "level": "INFO",
  "logger": "scraper_manager.orchestrator",
  "message": "Completed: 250 rows saved in 3.45s",
  "module": "orchestrator",
  "function": "process",
  "line": 89,
  "ticker": "AAPL",
  "operation": "process",
  "rows": 250,
  "duration_ms": 3450
}
```

### Health Checks

- `GET /healthz`: Liveness probe (always 200 if process is running)
- `GET /ready`: Readiness probe (200 if not processing, 503 if busy)
- `GET /metrics`: Prometheus metrics endpoint

## Testing

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run all tests
pytest tests/ -v

# Run with coverage
pip install pytest-cov
pytest tests/ --cov=scraper_manager --cov-report=html
```

## Troubleshooting

### Pods failing with OOMKilled

The v2.0 architecture is more memory-efficient than v1.0 (async vs threads). If you still see OOMKilled:

1. Check memory usage: `kubectl top pods -n apps-prod -l app=scraper-manager`
2. Increase memory limit in `deployment.yaml` (current: 1Gi)
3. Reduce `MAX_WORKERS` to lower concurrency

### Circuit breaker tripping frequently

1. Check yfinance wrapper health: `kubectl logs -n apps-prod -l app=yfinance-wrapper`
2. Check for rate limiting: Look for 429 status codes in logs
3. Increase `CIRCUIT_BREAKER_THRESHOLD` if false positives
4. Reduce `MAX_CONCURRENT_YFINANCE_CALLS` to ease pressure on wrapper

### Slow processing

1. Check network latency to services
2. Increase `MAX_WORKERS` (current: 8)
3. Increase `MAX_CONCURRENT_YFINANCE_CALLS` (current: 4) if yfinance wrapper can handle it
4. Check database service performance

### No data being saved

1. Check database service logs: `kubectl logs -n apps-prod -l app=finance-app-database-service`
2. Verify database connectivity from scraper pod
3. Check for constraint violations in logs
4. Verify ticker IDs exist in database

## Migration from v1.0

The v2.0 rewrite is **backwards compatible** with the v1.0 API contracts:

- Same environment variables (`DATABASE_SERVICE_URL`, `YFINANCE_SERVICE_URL`)
- Same database service endpoints (`/tickers/update-status`, `/history/batch`)
- Same yfinance wrapper endpoints (`/history?ticker_name=...&period=max` or `&start=...&end=...`)
- Same data format for transformation

**Breaking changes:**
- Docker image tag: `latest` → `v2.0` (update Helm chart)
- Health check port: None → 8080 (add probes to deployment)
- Log format: `print()` → structured JSON (update log parsing if needed)

## Future Improvements

1. **Distributed tracing**: Add OpenTelemetry for end-to-end request tracing
2. **Alerting**: Set up Prometheus alerts for high error rates or circuit breaker trips
3. **Incremental backfill**: Add ability to backfill specific date ranges on demand
4. **Ticker discovery**: Auto-discover new tickers from exchange APIs
5. **Data validation**: Add schema validation for yfinance responses
6. **Dead letter queue**: Store failed ticker updates for manual retry
