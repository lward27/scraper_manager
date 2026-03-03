import requests
from datetime import date, timedelta
from scraper_manager.config import DATABASE_SERVICE_URL, YFINANCE_SERVICE_URL

REQUEST_TIMEOUT = 30
BATCH_TIMEOUT = 60


def get_tickers_needing_update() -> list[dict]:
    """Single DB call: returns all tickers whose last price_history date is before yesterday."""
    r = requests.get(f"{DATABASE_SERVICE_URL}/tickers/update-status", timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    yesterday = date.today() - timedelta(days=1)
    return [t for t in r.json() if date.fromisoformat(t["last_date"]) < yesterday]


def fetch_max(ticker: str) -> dict | None:
    """Fetch the full available history for a ticker using period=max. Returns None on 404."""
    r = requests.get(
        f"{YFINANCE_SERVICE_URL}/history",
        params={"ticker_name": ticker, "period": "max"},
        timeout=REQUEST_TIMEOUT,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def fetch_chunk(ticker: str, start: date, end: date) -> dict | None:
    """Fetch one date-range window from yfinance. Returns raw OHLCV dict or None on 404."""
    r = requests.get(
        f"{YFINANCE_SERVICE_URL}/history",
        params={"ticker_name": ticker, "start": str(start), "end": str(end)},
        timeout=REQUEST_TIMEOUT,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def transform_chunk(raw: dict, ticker_id: int) -> list[dict]:
    """Pivot yfinance nested OHLCV dict into a flat list of row dicts for /history/batch."""
    opens   = raw.get("Open", {})
    highs   = raw.get("High", {})
    lows    = raw.get("Low", {})
    closes  = raw.get("Close", {})
    volumes = raw.get("Volume", {})

    rows = []
    for dt_str in opens:
        rows.append({
            "ticker_id": ticker_id,
            "ts":        dt_str[:10],   # trim to YYYY-MM-DD
            "open":      opens.get(dt_str),
            "high":      highs.get(dt_str),
            "low":       lows.get(dt_str),
            "close":     closes.get(dt_str),
            "volume":    volumes.get(dt_str),
        })
    return rows


def save_batch(rows: list[dict]) -> None:
    """POST a batch of OHLCV rows to the database service."""
    if not rows:
        return
    r = requests.post(
        f"{DATABASE_SERVICE_URL}/history/batch",
        json=rows,
        timeout=BATCH_TIMEOUT,
    )
    r.raise_for_status()
