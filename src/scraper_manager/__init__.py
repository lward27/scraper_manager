"""
Scraper Manager v2.0 - Production-grade stock data fetcher.

Async/await-based architecture with:
- Circuit breaker for yfinance wrapper resilience
- Structured JSON logging
- Prometheus-compatible metrics
- Incremental updates with smart chunking
- Backpressure via asyncio.Semaphore
- Idempotent batch saves with ON CONFLICT DO NOTHING
"""

__version__ = "2.0.0"
