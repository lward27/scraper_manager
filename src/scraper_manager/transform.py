"""
Data transformation utilities.

Converts yfinance API response format (nested OHLCV dict) into flat row dicts
suitable for the database service /history/batch endpoint.
"""

from typing import Optional


def transform_chunk(raw: dict, ticker_id: int) -> list[dict]:
    """
    Pivot yfinance nested OHLCV dict into a flat list of row dicts.

    Input format (from yfinance wrapper):
    {
        "Open": {"2024-01-01": 150.0, "2024-01-02": 151.0, ...},
        "High": {"2024-01-01": 152.0, ...},
        "Low": {"2024-01-01": 149.0, ...},
        "Close": {"2024-01-01": 151.5, ...},
        "Volume": {"2024-01-01": 1000000, ...}
    }

    Output format (for database service):
    [
        {
            "ticker_id": 1,
            "ts": "2024-01-01",
            "open": 150.0,
            "high": 152.0,
            "low": 149.0,
            "close": 151.5,
            "volume": 1000000
        },
        ...
    ]

    Args:
        raw: Raw OHLCV dict from yfinance wrapper.
        ticker_id: Database ticker ID.

    Returns:
        List of flat row dicts, sorted by date.
    """
    opens = raw.get("Open", {})
    highs = raw.get("High", {})
    lows = raw.get("Low", {})
    closes = raw.get("Close", {})
    volumes = raw.get("Volume", {})

    rows = []
    for dt_str in opens:
        # Trim to YYYY-MM-DD if timestamp includes time
        ts = dt_str[:10] if len(dt_str) > 10 else dt_str

        rows.append({
            "ticker_id": ticker_id,
            "ts": ts,
            "open": opens.get(dt_str),
            "high": highs.get(dt_str),
            "low": lows.get(dt_str),
            "close": closes.get(dt_str),
            "volume": volumes.get(dt_str),
        })

    # Sort by date for consistent ordering
    rows.sort(key=lambda r: r["ts"])

    return rows
