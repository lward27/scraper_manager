import requests
import json
from datetime import datetime, timedelta
from scraper_manager.config import DATABASE_SERVICE_URL, YFINANCE_SERVICE_URL

REQUEST_TIMEOUT_SECONDS = 10

# get tickers from db
def get_tickers(offset: int, limit: int):
    r = requests.get(
        f"{DATABASE_SERVICE_URL}/tickers?offset={offset}&limit={limit}",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    l = []
    for tick in r.json():
        l.append(tick["ticker"])
    return l

def get_ticker_count():
    r = requests.get(
        f"{DATABASE_SERVICE_URL}/tickers/count",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    count = r.json()
    return count

def get_period(ticker: str):
    r = requests.get(
        f"{DATABASE_SERVICE_URL}/history/last_date?ticker_name={ticker}",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if r.json():
        print(f"Latest Date for {ticker}: {r.json()}")
        latest_date = datetime.fromisoformat(r.json())
        todays_date = datetime.now()
        period = count_weekdays(latest_date, todays_date)
        print(f"Period set to: {period}d")
        return (str(period) + 'd'), latest_date
    else:
        print(f"{ticker}: No existing data, fetching max history")
        return 'max', None

# Note - this count will still include holidays, in order to handle holidays, an additional date check
# happens when transforming the payload.
def count_weekdays(start_date, end_date):
    number_of_days = (end_date - start_date).days
    number_of_weekdays = 0
    for i in range(number_of_days):
        date_to_check = start_date + timedelta(days=(i+1))
        if(date_to_check.isoweekday() <= 5):
            number_of_weekdays += 1
    return number_of_weekdays

def get_history(ticker: str, period: str):
    r = requests.get(
        f"{YFINANCE_SERVICE_URL}/history?ticker_name={ticker}&period={period}",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if r.status_code != 200:
        return "Ticker Not Found", r.status_code
    return r.json(), r.status_code

def save_batch_history(batch_history):
    r = requests.post(
        f"{DATABASE_SERVICE_URL}/history/batch",
        json=batch_history,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    return r.text, r.status_code

def transform_payload(payload: dict, ticker_name: str, latest_date) -> list:
    history_batch = []
    if latest_date is not None:
        latest_date = latest_date.replace(hour=0)

    for price_type in ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]:
        for _date, _price in payload[price_type].items():
            parsed_date = datetime.fromisoformat(_date[:19])
            if latest_date is None or parsed_date > latest_date:
                history_batch.append({
                    "price_type": price_type,
                    "datetime": _date,
                    "price": _price,
                    "ticker_name": ticker_name
                })
    return history_batch
