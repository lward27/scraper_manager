import sys
import time
import threading
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from scraper_manager import util

CHUNK_DAYS = 90
MAX_WORKERS = 8
START_OF_HISTORY = date(2000, 1, 1)

# Cap simultaneous yfinance calls to avoid overwhelming the wrapper
_yfinance_semaphore = threading.Semaphore(4)
SLEEP_BETWEEN_CHUNKS = 1.5  # seconds, inside the semaphore


def update_ticker(ticker_info: dict) -> tuple[str, int, str | None]:
    """Fetch and store all missing price history for one ticker in 90-day chunks.

    Returns (ticker_symbol, chunks_saved, error_message_or_None).
    """
    ticker_id  = ticker_info["ticker_id"]
    ticker_sym = ticker_info["ticker"]
    last_date  = date.fromisoformat(ticker_info["last_date"])
    yesterday  = date.today() - timedelta(days=1)

    start = START_OF_HISTORY if last_date <= date(1900, 1, 2) else last_date + timedelta(days=1)
    end   = yesterday
    chunks_saved = 0

    while start <= end:
        chunk_end = min(start + timedelta(days=CHUNK_DAYS), end)

        with _yfinance_semaphore:
            try:
                raw = util.fetch_chunk(ticker_sym, start, chunk_end)
            except Exception as e:
                return ticker_sym, chunks_saved, f"fetch error ({start}–{chunk_end}): {e}"
            time.sleep(SLEEP_BETWEEN_CHUNKS)

        if raw is None:
            # Ticker not recognised by yfinance — stop processing it
            return ticker_sym, chunks_saved, "not found in yfinance"

        rows = util.transform_chunk(raw, ticker_id)
        if rows:
            try:
                util.save_batch(rows)
            except Exception as e:
                return ticker_sym, chunks_saved, f"save error ({start}–{chunk_end}): {e}"
            chunks_saved += 1

        start = chunk_end + timedelta(days=1)

    return ticker_sym, chunks_saved, None


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
                print(f"[{completed}/{len(tickers)}] {sym}: {chunks} chunk(s) saved")

    if errors:
        print(f"\nCompleted with {len(errors)} error(s):")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)

    print("Scraper Manager complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
