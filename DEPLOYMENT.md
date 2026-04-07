# Scraper Manager v2.0 - Deployment Summary

## What Changed

### Architecture
- **Before**: Thread-based concurrency with `ThreadPoolExecutor(8)` + `Semaphore(4)`
- **After**: Async/await with `asyncio` + `asyncio.Semaphore` for non-blocking I/O

### New Components
1. **Circuit Breaker** (`circuit_breaker.py`): Trips after 10 consecutive failures, resets after 30s
2. **Structured Logging** (`logger.py`): JSON-formatted logs with context (ticker, operation, duration)
3. **Metrics** (`metrics.py`): Prometheus-compatible counters, gauges, and histograms
4. **Health Server** (`health_server.py`): HTTP server on port 8080 with `/healthz`, `/ready`, `/metrics`
5. **HTTP Client** (`http_client.py`): Async HTTP with retry logic and circuit breaker integration
6. **Orchestrator** (`orchestrator.py`): Main async loop coordinating fetch/save operations
7. **Transform** (`transform.py`): Pure function for yfinance response transformation

### Removed
- `util.py`: Replaced by modular architecture

### Configuration
All settings are environment-variable driven. New variables:
- `LOG_LEVEL` (default: INFO)
- `LOG_FORMAT` (default: json)
- `MAX_CONCURRENT_YFINANCE_CALLS` (default: 4)
- `MAX_CONCURRENT_DB_CALLS` (default: 4)
- `HEALTH_PORT` (default: 8080)
- `CIRCUIT_BREAKER_THRESHOLD` (default: 10)
- `CIRCUIT_BREAKER_RESET_TIMEOUT` (default: 30.0)

### Docker
- Non-root user for security
- Proper layer caching (dependencies installed before source)
- Health check port exposed (8080)

### Helm Chart
- Changed from CronJob to Job (can be wrapped in CronJob if scheduling needed)
- Added liveness probe: `GET /healthz:8080`
- Added readiness probe: `GET /ready:8080`
- Image tag: `v2.0` (was `latest`)
- Memory limits preserved: 512Mi request, 1Gi limit

## Files Changed

### scraper_manager repo
- `Dockerfile`: Updated with non-root user and layer caching
- `README.md`: Comprehensive architecture docs and deployment instructions
- `requirements.txt`: Changed from `requests` to `aiohttp`
- `src/setup.py`: Updated version to 2.0.0
- `src/scraper_manager/__init__.py`: Updated docstring
- `src/scraper_manager/__main__.py`: New entry point with health server and signal handling
- `src/scraper_manager/config.py`: Complete rewrite with dataclass-based config
- `src/scraper_manager/circuit_breaker.py`: New file
- `src/scraper_manager/health_server.py`: New file
- `src/scraper_manager/http_client.py`: New file
- `src/scraper_manager/logger.py`: New file
- `src/scraper_manager/metrics.py`: New file
- `src/scraper_manager/orchestrator.py`: New file
- `src/scraper_manager/transform.py`: New file (extracted from util.py)
- `src/scraper_manager/util.py`: Deleted (replaced by new modules)
- `tests/__init__.py`: New file
- `tests/test_transform.py`: New file (unit tests)
- `tests/test_circuit_breaker.py`: New file (unit tests)
- `tests/test_metrics.py`: New file (unit tests)
- `pyproject.toml`: New file (pytest config)

### lucas_engineering repo
- `charts/scraper-manager/deployment.yaml`: Updated for v2.0 with probes and new image tag

## Deployment Steps

### 1. Build and Push Docker Image
```bash
cd /home/openclaw/.openclaw/workspace/scraper_manager

# Build for linux/amd64
docker build --platform linux/amd64 -t registry.lucas.engineering/scraper_manager:v2.0 .

# Push to registry
docker push registry.lucas.engineering/scraper_manager:v2.0
```

### 2. ArgoCD Sync
ArgoCD will automatically detect the changes in the `lucas_engineering` repo and sync the updated deployment to the `apps-prod` namespace.

To manually trigger sync:
```bash
argocd app sync scraper-manager -n argocd
```

### 3. Verify Deployment
```bash
# Check pod status
kubectl get pods -n apps-prod -l app=scraper-manager

# Check logs
kubectl logs -n apps-prod -l app=scraper-manager -f

# Check metrics
kubectl port-forward -n apps-prod job/scraper-manager 8080:8080
curl http://localhost:8080/metrics
```

### 4. Run the Job
```bash
# Trigger manually
kubectl create job --from=cronjob/scraper-manager scraper-manager-manual -n apps-prod

# Or if using Job directly (as in the updated chart)
kubectl apply -f charts/scraper-manager/deployment.yaml -n apps-prod
```

## Rollback Plan

If v2.0 has issues, rollback to v1.0:

```bash
# In lucas_engineering repo
git revert HEAD  # Reverts the deployment.yaml change
git push origin main

# Or manually
kubectl rollout undo job/scraper-manager -n apps-prod
```

## Monitoring

### Key Metrics to Watch
- `scraper_tickers_success`: Should match total tickers processed
- `scraper_tickers_failed`: Should be 0 or very low
- `scraper_circuit_breaker_trips`: Should be 0 (indicates downstream issues)
- `scraper_fetch_duration_seconds`: Should be < 5s p95
- `scraper_save_duration_seconds`: Should be < 2s p95

### Alerting Recommendations
- Alert if `scraper_tickers_failed` > 10% of total
- Alert if `scraper_circuit_breaker_trips` > 0
- Alert if job duration > 1 hour (activeDeadlineSeconds is 4 hours)

## Testing

Run unit tests before deployment:
```bash
cd /home/openclaw/.openclaw/workspace/scraper_manager
pip install pytest pytest-asyncio
pytest tests/ -v
```

## Notes

- The v2.0 architecture is **backwards compatible** with v1.0 API contracts
- No database schema changes required
- No yfinance wrapper changes required
- The Job will complete and exit (unlike a long-running service)
- For scheduled runs, wrap the Job in a CronJob or use Kubernetes CronJob directly
