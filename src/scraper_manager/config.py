"""
Configuration management for scraper-manager.

All settings are environment-variable driven with sensible defaults
for production (K8s) and local development.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ServiceConfig:
    """Service endpoint configuration."""

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


@dataclass
class ConcurrencyConfig:
    """Concurrency and rate-limiting configuration."""

    max_workers: int = field(
        default_factory=lambda: int(os.getenv("MAX_WORKERS", "8"))
    )
    max_concurrent_yfinance_calls: int = field(
        default_factory=lambda: int(os.getenv("MAX_CONCURRENT_YFINANCE_CALLS", "4"))
    )
    max_concurrent_db_calls: int = field(
        default_factory=lambda: int(os.getenv("MAX_CONCURRENT_DB_CALLS", "4"))
    )


@dataclass
class RetryConfig:
    """Retry and circuit breaker configuration."""

    max_retries: int = field(
        default_factory=lambda: int(os.getenv("MAX_RETRIES", "5"))
    )
    base_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_BASE_DELAY", "2.0"))
    )
    max_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_MAX_DELAY", "60.0"))
    )
    # Circuit breaker: trip after this many consecutive failures
    circuit_breaker_threshold: int = field(
        default_factory=lambda: int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "10"))
    )
    # Circuit breaker: reset after this many seconds
    circuit_breaker_reset_timeout: float = field(
        default_factory=lambda: float(os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT", "30.0"))
    )


@dataclass
class ChunkConfig:
    """Data chunking configuration."""

    chunk_days: int = field(
        default_factory=lambda: int(os.getenv("CHUNK_DAYS", "90"))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "500"))
    )


@dataclass
class TimeoutConfig:
    """HTTP timeout configuration (seconds)."""

    request_timeout: int = field(
        default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT", "30"))
    )
    batch_timeout: int = field(
        default_factory=lambda: int(os.getenv("BATCH_TIMEOUT", "60"))
    )
    status_check_timeout: int = field(
        default_factory=lambda: int(os.getenv("STATUS_CHECK_TIMEOUT", "300"))
    )


@dataclass
class LoggingConfig:
    """Logging configuration."""

    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )
    log_format: str = field(
        default_factory=lambda: os.getenv("LOG_FORMAT", "json")
    )


@dataclass
class Config:
    """Root configuration object."""

    services: ServiceConfig = field(default_factory=ServiceConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    chunk: ChunkConfig = field(default_factory=ChunkConfig)
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls()
