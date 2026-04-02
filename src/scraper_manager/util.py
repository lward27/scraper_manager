import requests
import time
import random
from datetime import date, timedelta
from scraper_manager.config import DATABASE_SERVICE_URL, YFINANCE_SERVICE_URL

REQUEST_TIMEOUT = 30
BATCH_TIMEOUT = 60
# Much longer timeout for the initial status check - query is expensive with 5647 tickers
STATUS_CHECK_TIMEOUT = 600

# Retry configuration
MAX_RETRIES = 5
BASE_DELAY = 2  # seconds
MAX_DELAY = 60  # seconds


def _fetch_with_retry(url: str, params: dict | None = None, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """Make HTTP GET with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            # Don't retry on 404 (legitimate not found)
            if r.status_code == 404:
                return r
            # Retry on 429 (rate limit) or 5xx errors
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < MAX_RETRIES - 1:
                    delay = min(BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_DELAY)
                    print(f"  Retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s (status {r.status_code})")
                    time.sleep(delay)
                    continue
            return r
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                delay = min(BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_DELAY)
                print(f"  Timeout, retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = min(BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_DELAY)
                print(f"  Error: {e}, retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
    return r


def get_tickers_needing_update() -> list[dict]:
    """Single DB call: returns all tickers whose last price_history date is before yesterday."""
    r = requests.get(f"{DATABASE_SERVICE_URL}/tickers/update-status", timeout=STATUS_CHECK_TIMEOUT)
    r.raise_for_status()
    yesterday = date.today() - timedelta(days=1)
    return [t for t in r.json() if date.fromisoformat(t["last_date"]) < yesterday]


def fetch_max(ticker: str) -> dict | None:
    """Fetch the full available history for a ticker using period=max. Returns None on 404."""
    r = _fetch_with_retry(
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
    r = _fetch_with_retry(
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
    """POST a batch of OHLCV rows to the database service. Chunks large batches to 500 rows."""
    if not rows:
        return
    
    CHUNK_SIZE = 500
    
    # If batch is small enough, send as-is
    if len(rows) <= CHUNK_SIZE:
        r = requests.post(
            f"{DATABASE_SERVICE_URL}/history/batch",
            json=rows,
            timeout=BATCH_TIMEOUT,
        )
        r.raise_for_status()
        return
    
    # Chunk large batches
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        r = requests.post(
            f"{DATABASE_SERVICE_URL}/history/batch",
            json=chunk,
            timeout=BATCH_TIMEOUT,
        )
        r.raise_for_status()
        print(f"  Saved chunk {i//CHUNK_SIZE + 1}/{(len(rows) + CHUNK_SIZE - 1)//CHUNK_SIZE} ({len(chunk)} rows)")
