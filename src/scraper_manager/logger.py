"""
Structured JSON logger for scraper-manager.

Provides consistent log formatting with context (ticker, operation, etc.)
and optional file output for debugging.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging pipelines."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
            }

        # Add extra context fields
        if hasattr(record, "ticker"):
            log_entry["ticker"] = record.ticker
        if hasattr(record, "operation"):
            log_entry["operation"] = record.operation
        if hasattr(record, "rows"):
            log_entry["rows"] = record.rows
        if hasattr(record, "duration_ms"):
            log_entry["duration_ms"] = record.duration_ms

        return json.dumps(log_entry)


class ContextAdapter(logging.LoggerAdapter):
    """Logger adapter that adds context to log records."""

    def process(
        self, msg: str, kwargs: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        extra = self.extra.copy() if self.extra else {}
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(
    name: str = "scraper_manager",
    level: str = "INFO",
    json_format: bool = True,
) -> ContextAdapter:
    """
    Create a logger with optional JSON formatting.

    Args:
        name: Logger name (usually __name__).
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_format: If True, use JSON formatter; otherwise use standard format.

    Returns:
        ContextAdapter for structured logging with context support.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        if json_format:
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )

        logger.addHandler(handler)

    return ContextAdapter(logger, {})


def with_context(
    logger: ContextAdapter, **kwargs: Any
) -> ContextAdapter:
    """
    Create a new logger adapter with additional context.

    Usage:
        log = get_logger(__name__)
        log = with_context(log, ticker="AAPL", operation="fetch")
        log.info("Starting fetch")  # includes ticker and operation in JSON
    """
    extra = logger.extra.copy() if logger.extra else {}
    extra.update(kwargs)
    return ContextAdapter(logger.logger, extra)
