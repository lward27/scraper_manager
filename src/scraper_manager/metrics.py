"""
Metrics collection for scraper-manager.

Tracks counters, gauges, and histograms for observability.
Exports in Prometheus text format for scraping by Prometheus.
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Counter:
    """Monotonically increasing counter."""

    name: str
    description: str
    value: int = 0
    labels: dict[str, str] = field(default_factory=dict)

    def inc(self, amount: int = 1) -> None:
        self.value += amount

    def render(self) -> str:
        label_str = ",".join(f'{k}="{v}"' for k, v in self.labels.items())
        if label_str:
            return f"# HELP {self.name} {self.description}\n# TYPE {self.name} counter\n{self.name}{{{label_str}}} {self.value}"
        return f"# HELP {self.name} {self.description}\n# TYPE {self.name} counter\n{self.name} {self.value}"


@dataclass
class Gauge:
    """Value that can go up and down."""

    name: str
    description: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)

    def set(self, value: float) -> None:
        self.value = value

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount

    def dec(self, amount: float = 1.0) -> None:
        self.value -= amount

    def render(self) -> str:
        label_str = ",".join(f'{k}="{v}"' for k, v in self.labels.items())
        if label_str:
            return f"# HELP {self.name} {self.description}\n# TYPE {self.name} gauge\n{self.name}{{{label_str}}} {self.value}"
        return f"# HELP {self.name} {self.description}\n# TYPE {self.name} gauge\n{self.name} {self.value}"


@dataclass
class Histogram:
    """Tracks distribution of values (e.g., latencies)."""

    name: str
    description: str
    buckets: list[float] = field(
        default_factory=lambda: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sum_value: float = 0.0
    count: int = 0

    def observe(self, value: float) -> None:
        self.sum_value += value
        self.count += 1
        for bucket in self.buckets:
            if value <= bucket:
                self.counts[f"le_{bucket}"] += 1
        self.counts["le_inf"] += 1

    def render(self) -> str:
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} histogram",
        ]
        for bucket in self.buckets:
            key = f"le_{bucket}"
            lines.append(f'{self.name}_bucket{{le="{bucket}"}} {self.counts.get(key, 0)}')
        lines.append(f'{self.name}_bucket{{le="+Inf"}} {self.counts.get("le_inf", 0)}')
        lines.append(f"{self.name}_sum {self.sum_value}")
        lines.append(f"{self.name}_count {self.count}")
        return "\n".join(lines)


class MetricsRegistry:
    """Central registry for all metrics."""

    def __init__(self):
        self.counters: dict[str, Counter] = {}
        self.gauges: dict[str, Gauge] = {}
        self.histograms: dict[str, Histogram] = {}

    def counter(self, name: str, description: str, labels: Optional[dict[str, str]] = None) -> Counter:
        if name not in self.counters:
            self.counters[name] = Counter(name=name, description=description, labels=labels or {})
        return self.counters[name]

    def gauge(self, name: str, description: str, labels: Optional[dict[str, str]] = None) -> Gauge:
        if name not in self.gauges:
            self.gauges[name] = Gauge(name=name, description=description, labels=labels or {})
        return self.gauges[name]

    def histogram(self, name: str, description: str, buckets: Optional[list[float]] = None) -> Histogram:
        if name not in self.histograms:
            self.histograms[name] = Histogram(name=name, description=description, buckets=buckets or [])
        return self.histograms[name]

    def render_prometheus(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        parts = []
        for counter in self.counters.values():
            parts.append(counter.render())
        for gauge in self.gauges.values():
            parts.append(gauge.render())
        for histogram in self.histograms.values():
            parts.append(histogram.render())
        return "\n\n".join(parts) + "\n"


# Global metrics registry
metrics = MetricsRegistry()

# Pre-register standard metrics
metrics.counter("scraper_tickers_total", "Total number of tickers processed")
metrics.counter("scraper_tickers_success", "Number of tickers successfully processed")
metrics.counter("scraper_tickers_failed", "Number of tickers that failed processing")
metrics.counter("scraper_rows_saved", "Total number of price history rows saved")
metrics.counter("scraper_api_calls_total", "Total API calls made", labels={"service": "yfinance"})
metrics.counter("scraper_api_errors_total", "Total API errors", labels={"service": "yfinance"})
metrics.counter("scraper_db_writes_total", "Total database write operations")
metrics.counter("scraper_db_errors_total", "Total database write errors")
metrics.counter("scraper_circuit_breaker_trips", "Number of times circuit breaker tripped")

metrics.gauge("scraper_active_workers", "Number of currently active worker tasks")
metrics.gauge("scraper_queue_depth", "Number of tickers waiting to be processed")

metrics.histogram("scraper_fetch_duration_seconds", "Duration of yfinance fetch calls")
metrics.histogram("scraper_save_duration_seconds", "Duration of database save operations")
metrics.histogram("scraper_ticker_duration_seconds", "Total duration to process a single ticker")


class Timer:
    """Context manager for timing operations and recording to a histogram."""

    def __init__(self, histogram: Histogram):
        self.histogram = histogram
        self.start_time: Optional[float] = None

    def __enter__(self):
        self.start_time = time.monotonic()
        return self

    def __exit__(self, *args):
        if self.start_time is not None:
            elapsed = time.monotonic() - self.start_time
            self.histogram.observe(elapsed)
