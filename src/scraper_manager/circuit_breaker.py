"""
Circuit breaker implementation for resilient API calls.

Prevents cascading failures when a downstream service (yfinance wrapper
or database service) is unhealthy.
"""

import time
import asyncio
from enum import Enum
from typing import Optional, Callable, Any
from dataclasses import dataclass, field

from scraper_manager.logger import get_logger, with_context

log = get_logger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Tripped, requests fail fast
    HALF_OPEN = "half_open" # Testing if service recovered


@dataclass
class CircuitBreaker:
    """
    Circuit breaker for async HTTP calls.

    Trips after `failure_threshold` consecutive failures.
    Resets to CLOSED after `reset_timeout` seconds in OPEN state.
    In HALF_OPEN, allows one test request; success closes, failure re-opens.
    """

    name: str
    failure_threshold: int = 10
    reset_timeout: float = 30.0

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: Optional[float] = field(default=None, init=False)
    _success_count_half_open: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        """Check if we should transition from OPEN to HALF_OPEN."""
        if self._state == CircuitState.OPEN and self._last_failure_time:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.reset_timeout:
                log.logger.info(
                    f"Circuit breaker '{self.name}' transitioning OPEN -> HALF_OPEN "
                    f"(reset timeout {self.reset_timeout}s elapsed)"
                )
                self._state = CircuitState.HALF_OPEN
                self._success_count_half_open = 0
        return self._state

    def _record_success(self):
        if self._state == CircuitState.HALF_OPEN:
            self._success_count_half_open += 1
            # Close after first success in half-open
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            log.logger.info(f"Circuit breaker '{self.name}' CLOSED (recovered)")
        else:
            self._failure_count = 0

    def _record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            log.logger.warning(
                f"Circuit breaker '{self.name}' re-OPENED after failure in HALF_OPEN"
            )
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            log.logger.warning(
                f"Circuit breaker '{self.name}' OPENED after {self._failure_count} failures"
            )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute an async function through the circuit breaker.

        Raises CircuitBreakerOpen if the circuit is open.
        Propagates exceptions from the wrapped function.
        """
        current_state = self.state

        if current_state == CircuitState.OPEN:
            raise CircuitBreakerOpen(
                f"Circuit breaker '{self.name}' is OPEN. "
                f"Failures: {self._failure_count}, "
                f"Last failure: {self._last_failure_time}s ago"
            )

        try:
            result = await func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            raise


class CircuitBreakerOpen(Exception):
    """Raised when a circuit breaker is in OPEN state."""
    pass


# Global circuit breakers
yfinance_circuit = CircuitBreaker(
    name="yfinance_wrapper",
    failure_threshold=10,
    reset_timeout=30.0,
)

database_circuit = CircuitBreaker(
    name="database_service",
    failure_threshold=10,
    reset_timeout=30.0,
)
