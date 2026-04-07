"""
Unit tests for scraper-manager transformation logic.

Tests are pure unit tests with no external dependencies.
"""

import pytest
from datetime import date

from scraper_manager.transform import transform_chunk


class TestTransformChunk:
    """Tests for the transform_chunk function."""

    def test_basic_transformation(self):
        """Test basic OHLCV transformation."""
        raw = {
            "Open": {"2024-01-01": 150.0, "2024-01-02": 151.0},
            "High": {"2024-01-01": 152.0, "2024-01-02": 153.0},
            "Low": {"2024-01-01": 149.0, "2024-01-02": 150.0},
            "Close": {"2024-01-01": 151.5, "2024-01-02": 152.5},
            "Volume": {"2024-01-01": 1000000, "2024-01-02": 1100000},
        }

        rows = transform_chunk(raw, ticker_id=1)

        assert len(rows) == 2
        assert rows[0] == {
            "ticker_id": 1,
            "ts": "2024-01-01",
            "open": 150.0,
            "high": 152.0,
            "low": 149.0,
            "close": 151.5,
            "volume": 1000000,
        }
        assert rows[1] == {
            "ticker_id": 1,
            "ts": "2024-01-02",
            "open": 151.0,
            "high": 153.0,
            "low": 150.0,
            "close": 152.5,
            "volume": 1100000,
        }

    def test_empty_input(self):
        """Test transformation with empty input."""
        raw = {}
        rows = transform_chunk(raw, ticker_id=1)
        assert rows == []

    def test_missing_fields(self):
        """Test transformation when some fields are missing."""
        raw = {
            "Open": {"2024-01-01": 150.0},
            "High": {},
            "Low": {},
            "Close": {},
            "Volume": {},
        }

        rows = transform_chunk(raw, ticker_id=1)

        assert len(rows) == 1
        assert rows[0]["open"] == 150.0
        assert rows[0]["high"] is None
        assert rows[0]["low"] is None
        assert rows[0]["close"] is None
        assert rows[0]["volume"] is None

    def test_sorted_output(self):
        """Test that output is sorted by date."""
        raw = {
            "Open": {"2024-01-03": 150.0, "2024-01-01": 148.0, "2024-01-02": 149.0},
            "High": {"2024-01-03": 152.0, "2024-01-01": 150.0, "2024-01-02": 151.0},
            "Low": {"2024-01-03": 149.0, "2024-01-01": 147.0, "2024-01-02": 148.0},
            "Close": {"2024-01-03": 151.0, "2024-01-01": 149.0, "2024-01-02": 150.0},
            "Volume": {"2024-01-03": 1000000, "2024-01-01": 900000, "2024-01-02": 950000},
        }

        rows = transform_chunk(raw, ticker_id=1)

        assert len(rows) == 3
        assert rows[0]["ts"] == "2024-01-01"
        assert rows[1]["ts"] == "2024-01-02"
        assert rows[2]["ts"] == "2024-01-03"

    def test_timestamp_truncation(self):
        """Test that timestamps with time component are truncated to date."""
        raw = {
            "Open": {"2024-01-01T00:00:00": 150.0},
            "High": {"2024-01-01T00:00:00": 152.0},
            "Low": {"2024-01-01T00:00:00": 149.0},
            "Close": {"2024-01-01T00:00:00": 151.5},
            "Volume": {"2024-01-01T00:00:00": 1000000},
        }

        rows = transform_chunk(raw, ticker_id=1)

        assert len(rows) == 1
        assert rows[0]["ts"] == "2024-01-01"

    def test_ticker_id_preserved(self):
        """Test that ticker_id is correctly set."""
        raw = {
            "Open": {"2024-01-01": 150.0},
            "High": {"2024-01-01": 152.0},
            "Low": {"2024-01-01": 149.0},
            "Close": {"2024-01-01": 151.5},
            "Volume": {"2024-01-01": 1000000},
        }

        rows = transform_chunk(raw, ticker_id=42)

        assert all(row["ticker_id"] == 42 for row in rows)

    def test_none_values_handled(self):
        """Test that None values in input are preserved."""
        raw = {
            "Open": {"2024-01-01": None},
            "High": {"2024-01-01": None},
            "Low": {"2024-01-01": None},
            "Close": {"2024-01-01": None},
            "Volume": {"2024-01-01": None},
        }

        rows = transform_chunk(raw, ticker_id=1)

        assert len(rows) == 1
        assert rows[0]["open"] is None
        assert rows[0]["high"] is None
        assert rows[0]["low"] is None
        assert rows[0]["close"] is None
        assert rows[0]["volume"] is None
