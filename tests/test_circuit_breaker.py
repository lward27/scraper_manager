"""
Unit tests for circuit breaker logic.
"""

import pytest
import asyncio
import time

from scraper_manager.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpen


class TestCircuitBreaker:
    """Tests for the CircuitBreaker class."""

    def test_initial_state_closed(self):
        """Circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_trips_after_threshold_failures(self):
        """Circuit breaker trips after reaching failure threshold."""
        cb = CircuitBreaker(name="test", failure_threshold=3, reset_timeout=1.0)

        # Record failures up to threshold
        for _ in range(3):
            cb._record_failure()

        assert cb.state == CircuitState.OPEN

    def test_resets_after_timeout(self):
        """Circuit breaker transitions to HALF_OPEN after reset timeout."""
        cb = CircuitBreaker(name="test", failure_threshold=2, reset_timeout=0.1)

        # Trip the circuit
        for _ in range(2):
            cb._record_failure()
        assert cb.state == CircuitState.OPEN

        # Wait for reset timeout
        time.sleep(0.15)

        # Should transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN

    def test_closes_on_success_in_half_open(self):
        """Circuit breaker closes after success in HALF_OPEN state."""
        cb = CircuitBreaker(name="test", failure_threshold=2, reset_timeout=0.1)

        # Trip the circuit
        for _ in range(2):
            cb._record_failure()
        assert cb.state == CircuitState.OPEN

        # Wait for reset timeout
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        # Record success
        cb._record_success()
        assert cb.state == CircuitState.CLOSED

    def test_reopens_on_failure_in_half_open(self):
        """Circuit breaker re-opens after failure in HALF_OPEN state."""
        cb = CircuitBreaker(name="test", failure_threshold=2, reset_timeout=0.1)

        # Trip the circuit
        for _ in range(2):
            cb._record_failure()

        # Wait for reset timeout
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        # Record failure
        cb._record_failure()
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_call_success(self):
        """Circuit breaker allows successful calls."""
        cb = CircuitBreaker(name="test", failure_threshold=3)

        async def successful_func():
            return "success"

        result = await cb.call(successful_func)
        assert result == "success"

    @pytest.mark.asyncio
    async def test_call_failure(self):
        """Circuit breaker propagates exceptions from failed calls."""
        cb = CircuitBreaker(name="test", failure_threshold=3)

        async def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            await cb.call(failing_func)

    @pytest.mark.asyncio
    async def test_call_blocked_when_open(self):
        """Circuit breaker blocks calls when OPEN."""
        cb = CircuitBreaker(name="test", failure_threshold=2)

        # Trip the circuit
        for _ in range(2):
            cb._record_failure()

        async def some_func():
            return "should not reach"

        with pytest.raises(CircuitBreakerOpen):
            await cb.call(some_func)

    @pytest.mark.asyncio
    async def test_call_records_success(self):
        """Successful call resets failure count."""
        cb = CircuitBreaker(name="test", failure_threshold=3)

        # Record some failures
        for _ in range(2):
            cb._record_failure()
        assert cb._failure_count == 2

        # Successful call
        async def success_func():
            return "ok"
        await cb.call(success_func)

        assert cb._failure_count == 0

    @pytest.mark.asyncio
    async def test_call_records_failure(self):
        """Failed call increments failure count."""
        cb = CircuitBreaker(name="test", failure_threshold=3)

        async def fail_func():
            raise RuntimeError("fail")

        for i in range(3):
            with pytest.raises(RuntimeError):
                await cb.call(fail_func)
            assert cb._failure_count == i + 1

        # Should be open now
        assert cb.state == CircuitState.OPEN
