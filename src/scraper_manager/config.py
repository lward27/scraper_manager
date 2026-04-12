"""Configuration for scraper-manager queue runtime."""

import os
from dataclasses import dataclass, field


def _as_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int_list(value: str, default: list[int]) -> list[int]:
    if not value:
        return default
    result = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        result.append(int(chunk))
    return result if result else default


@dataclass
class ServiceConfig:
    database_service_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_SERVICE_URL",
            "http://finance-app-database-service.apps-prod.svc.cluster.local:8091",
        )
    )
    yfinance_service_url: str = field(
        default_factory=lambda: os.getenv(
            "YFINANCE_SERVICE_URL",
            "http://yfinance-wrapper.apps-prod.svc.cluster.local:8090",
        )
    )
    rabbitmq_url: str = field(
        default_factory=lambda: os.getenv(
            "RABBITMQ_URL",
            "amqp://finance_app_user:finance_app_user@rabbitmq.apps-prod.svc.cluster.local:5672/",
        )
    )


@dataclass
class QueueConfig:
    work_queue: str = field(default_factory=lambda: os.getenv("RABBITMQ_WORK_QUEUE", "scraper.ticker.work"))
    retry_queue: str = field(default_factory=lambda: os.getenv("RABBITMQ_RETRY_QUEUE", "scraper.ticker.retry"))
    dlq_queue: str = field(default_factory=lambda: os.getenv("RABBITMQ_DLQ_QUEUE", "scraper.ticker.dlq"))
    prefetch_count: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_PREFETCH_COUNT", "8")))


@dataclass
class SchedulerConfig:
    timezone: str = field(default_factory=lambda: os.getenv("SCHEDULER_TIMEZONE", "America/New_York"))
    run_hour: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_RUN_HOUR", "19")))
    run_minute: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_RUN_MINUTE", "0")))
    weekdays: list[int] = field(default_factory=lambda: _as_int_list(os.getenv("SCHEDULER_WEEKDAYS", "0,1,2,3,4"), [0, 1, 2, 3, 4]))
    poll_interval_seconds: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_POLL_SECONDS", "30")))
    page_size: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_PAGE_SIZE", "500")))
    target_date_offset_days: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_TARGET_DATE_OFFSET_DAYS", "0")))


@dataclass
class WorkerConfig:
    chunk_days: int = field(default_factory=lambda: int(os.getenv("CHUNK_DAYS", "90")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "500")))
    request_timeout: int = field(default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT", "30")))
    batch_timeout: int = field(default_factory=lambda: int(os.getenv("BATCH_TIMEOUT", "60")))
    status_check_timeout: int = field(default_factory=lambda: int(os.getenv("STATUS_CHECK_TIMEOUT", "120")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("WORKER_MAX_RETRIES", "5")))
    retry_base_delay_seconds: float = field(default_factory=lambda: float(os.getenv("WORKER_RETRY_BASE_DELAY_SECONDS", "10")))
    retry_max_delay_seconds: float = field(default_factory=lambda: float(os.getenv("WORKER_RETRY_MAX_DELAY_SECONDS", "300")))


@dataclass
class RuntimeConfig:
    mode: str = field(default_factory=lambda: os.getenv("MODE", "worker").strip().lower())
    shadow_mode: bool = field(default_factory=lambda: _as_bool(os.getenv("SHADOW_MODE", "true"), True))
    health_port: int = field(default_factory=lambda: int(os.getenv("HEALTH_PORT", "8080")))


@dataclass
class LoggingConfig:
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())
    log_format: str = field(default_factory=lambda: os.getenv("LOG_FORMAT", "json").strip().lower())


@dataclass
class Config:
    services: ServiceConfig = field(default_factory=ServiceConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_env(cls) -> "Config":
        return cls()
