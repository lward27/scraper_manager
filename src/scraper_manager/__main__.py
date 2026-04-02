import sys
import time
import threading
import random
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from scraper_manager import util

CHUNK_DAYS = 90
MAX_WORKERS = 8

# Cap simultaneous yfinance calls to avoid overwhelming the wrapper
_yfinance_semaphore = threading.Semaphore(4)
SLEEP_BETWEEN_CHUNKS = 1.5  # seconds, inside the semaphore
SLEEP_JITTER = 0.5  # +/- jitter to avoid predictable patterns


def update_ticker(ticker_info: dict) -> tuple[str, int, str | None]:
    """Fetch and store all missing price history for one ticker.

    New tickers (no prior data): single period=max call — avoids 404s from
    requesting date ranges before the ticker was listed.
    Existing tickers: 90-day chunks from last_date+1 to yesterday; a 404 on a
    chunk means no data in that window (e.g. trading halt) — skip and continue.

    Returns (ticker_symbol, rows_saved, error_message_or_None).
    """
    ticker_id  = ticker_info["ticker_id"]
    ticker_sym = ticker_info["ticker"]
    last_date  = date.fromisoformat(ticker_info["last_date"])
    yesterday  = date.today() - timedelta(days=1)

    is_new = last_date <= date(1900, 1, 2)

    if is_new:
        with _yfinance_semaphore:
            try:
                raw = util.fetch_max(ticker_sym)
            except Exception as e:
                return ticker_sym, 0, f"fetch error (max): {e}"
            # Add jitter to sleep to avoid predictable patterns
            sleep_time = SLEEP_BETWEEN_CHUNKS + random.uniform(-SLEEP_JITTER, SLEEP_JITTER)
            time.sleep(max(0, sleep_time))

        if raw is None:
            return ticker_sym, 0, "not found in yfinance"

        rows = util.transform_chunk(raw, ticker_id)
        if not rows:
            return ticker_sym, 0, None
        try:
            util.save_batch(rows)
        except Exception as e:
            return ticker_sym, 0, f"save error (max): {e}"
        return ticker_sym, len(rows), None

    # Incremental update: chunk from last_date+1 to yesterday
    start = last_date + timedelta(days=1)
    end   = yesterday
    rows_saved = 0

    while start <= end:
        chunk_end = min(start + timedelta(days=CHUNK_DAYS), end)

        with _yfinance_semaphore:
            try:
                raw = util.fetch_chunk(ticker_sym, start, chunk_end)
            except Exception as e:
                return ticker_sym, rows_saved, f"fetch error ({start}–{chunk_end}): {e}"
            # Add jitter to sleep to avoid predictable patterns
            sleep_time = SLEEP_BETWEEN_CHUNKS + random.uniform(-SLEEP_JITTER, SLEEP_JITTER)
            time.sleep(max(0, sleep_time))

        if raw is not None:
            rows = util.transform_chunk(raw, ticker_id)
            if rows:
                try:
                    util.save_batch(rows)
                except Exception as e:
                    return ticker_sym, rows_saved, f"save error ({start}–{chunk_end}): {e}"
                rows_saved += len(rows)

        start = chunk_end + timedelta(days=1)

    return ticker_sym, rows_saved, None


def main():
    print("Scraper Manager starting")

    try:
        tickers = util.get_tickers_needing_update()
    except Exception as e:
        print(f"Fatal: could not fetch update status — {e}")
        sys.exit(1)

    print(f"{len(tickers)} tickers need updating")
    if not tickers:
        print("Nothing to do.")
        sys.exit(0)

    errors = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(update_ticker, t): t["ticker"] for t in tickers}
        for future in as_completed(futures):
            sym, chunks, err = future.result()
            completed += 1
            if err:
                print(f"[{completed}/{len(tickers)}] {sym}: {err}")
                errors.append(f"{sym}: {err}")
            else:
                print(f"[{completed}/{len(tickers)}] {sym}: {chunks} row(s) saved")

    if errors:
        print(f"\nCompleted with {len(errors)} error(s):")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)

    print("Scraper Manager complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
