"""
Unit tests for metrics collection.
"""

import pytest

from scraper_manager.metrics import Counter, Gauge, Histogram, MetricsRegistry, Timer


class TestCounter:
    """Tests for Counter metric."""

    def test_initial_value(self):
        counter = Counter(name="test", description="test counter")
        assert counter.value == 0

    def test_increment(self):
        counter = Counter(name="test", description="test counter")
        counter.inc()
        assert counter.value == 1
        counter.inc(5)
        assert counter.value == 6

    def test_render_no_labels(self):
        counter = Counter(name="test_counter", description="A test counter")
        counter.inc(10)
        output = counter.render()
        assert "# HELP test_counter A test counter" in output
        assert "# TYPE test_counter counter" in output
        assert "test_counter 10" in output

    def test_render_with_labels(self):
        counter = Counter(name="test_counter", description="A test counter", labels={"service": "api"})
        counter.inc(5)
        output = counter.render()
        assert 'test_counter{service="api"} 5' in output


class TestGauge:
    """Tests for Gauge metric."""

    def test_initial_value(self):
        gauge = Gauge(name="test", description="test gauge")
        assert gauge.value == 0.0

    def test_set(self):
        gauge = Gauge(name="test", description="test gauge")
        gauge.set(42.5)
        assert gauge.value == 42.5

    def test_inc_dec(self):
        gauge = Gauge(name="test", description="test gauge")
        gauge.inc(10)
        assert gauge.value == 10.0
        gauge.dec(3)
        assert gauge.value == 7.0

    def test_render(self):
        gauge = Gauge(name="test_gauge", description="A test gauge")
        gauge.set(123.45)
        output = gauge.render()
        assert "# HELP test_gauge A test gauge" in output
        assert "# TYPE test_gauge gauge" in output
        assert "test_gauge 123.45" in output


class TestHistogram:
    """Tests for Histogram metric."""

    def test_observe(self):
        hist = Histogram(name="test", description="test histogram", buckets=[1.0, 5.0, 10.0])
        hist.observe(0.5)
        hist.observe(3.0)
        hist.observe(7.0)

        assert hist.count == 3
        assert hist.sum_value == 10.5
        assert hist.counts["le_1.0"] == 1  # 0.5 <= 1.0
        assert hist.counts["le_5.0"] == 2  # 0.5, 3.0 <= 5.0
        assert hist.counts["le_10.0"] == 3  # all <= 10.0
        assert hist.counts["le_inf"] == 3

    def test_render(self):
        hist = Histogram(name="test_hist", description="A test histogram", buckets=[1.0, 5.0])
        hist.observe(0.5)
        hist.observe(3.0)

        output = hist.render()
        assert "# HELP test_hist A test histogram" in output
        assert "# TYPE test_hist histogram" in output
        assert 'test_hist_bucket{le="1.0"} 1' in output
        assert 'test_hist_bucket{le="5.0"} 2' in output
        assert 'test_hist_bucket{le="+Inf"} 2' in output
        assert "test_hist_sum" in output
        assert "test_hist_count 2" in output


class TestMetricsRegistry:
    """Tests for MetricsRegistry."""

    def test_counter_registration(self):
        registry = MetricsRegistry()
        counter = registry.counter("test_counter", "A test counter")
        assert counter.name == "test_counter"
        assert counter.value == 0

    def test_gauge_registration(self):
        registry = MetricsRegistry()
        gauge = registry.gauge("test_gauge", "A test gauge")
        assert gauge.name == "test_gauge"
        assert gauge.value == 0.0

    def test_histogram_registration(self):
        registry = MetricsRegistry()
        hist = registry.histogram("test_hist", "A test histogram")
        assert hist.name == "test_hist"

    def test_render_prometheus(self):
        registry = MetricsRegistry()
        registry.counter("test_counter", "A counter").inc(5)
        registry.gauge("test_gauge", "A gauge").set(10.0)

        output = registry.render_prometheus()
        assert "test_counter" in output
        assert "test_gauge" in output


class TestTimer:
    """Tests for Timer context manager."""

    def test_timer_records_duration(self):
        hist = Histogram(name="test", description="test", buckets=[0.001, 0.01, 0.1, 1.0])
        
        with Timer(hist):
            pass  # Minimal sleep

        assert hist.count >= 1
        assert hist.sum_value > 0
