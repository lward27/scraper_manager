# Scraper Manager v3.0 Deployment Notes

## What Changed

- Replaced batch `Job` runtime with queue-driven long-running services.
- Added scheduler mode and worker mode to the same container image.
- Added RabbitMQ queue dependency for work/retry/DLQ flows.
- Added scraper run tracking integration with `finance_app_database_service`.

## Kubernetes Expectations

- Deployments:
  - `scraper-manager-scheduler`
  - `scraper-manager-worker`
- RabbitMQ service reachable from `apps-prod` namespace.
- `finance-app-database-service` healthy and connected to finance Postgres database.

## Rollout Sequence

1. Deploy/update Postgres tenancy + finance DB service changes.
2. Deploy RabbitMQ.
3. Deploy scraper scheduler/worker in `SHADOW_MODE=true`.
4. Validate queue behavior and freshness for 3 market days.
5. Set `SHADOW_MODE=false` on workers for cutover.

## Health Endpoints

- `/healthz`
- `/ready`
- `/metrics`
