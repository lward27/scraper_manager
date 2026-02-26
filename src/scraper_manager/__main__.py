from scraper_manager import util
import sys
import time


class ScrapeState:
    def __init__(self):
        self.is_running = False
        self.should_stop = False
        self.total_tickers = 0
        self.processed_tickers = 0
        self.current_ticker = None
        self.last_error = None
        self.started_at = None
        self.completed_at = None


scrape_state = ScrapeState()


def run_scrape_job():
    """Run the scrape job synchronously"""
    global scrape_state

    try:
        scrape_state.total_tickers = util.get_ticker_count()
        scrape_state.started_at = time.strftime("%Y-%m-%d %H:%M:%S")
        scrape_state.completed_at = None
        scrape_state.processed_tickers = 0
        scrape_state.last_error = None

        print(f"Total number of tickers to scrape: {scrape_state.total_tickers}")
        start_ticker = 0

        while start_ticker < scrape_state.total_tickers and not scrape_state.should_stop:
            # Grab 10 tickers at a time
            tickers = util.get_tickers(start_ticker, 10)
            print(f"\nProcessing Batch: {tickers}")
            start_ticker += 10

            for ticker in tickers:
                if scrape_state.should_stop:
                    break

                scrape_state.current_ticker = ticker

                try:
                    check_period, latest_date = util.get_period(ticker)

                    if check_period is None:
                        print(f"{ticker} returns no period - probably not in yfinance system, skipping!")
                        scrape_state.processed_tickers += 1
                        continue
                    if check_period == '0d':
                        print(f"{ticker} returns 0d period - already updated today")
                        scrape_state.processed_tickers += 1
                        continue

                    payload, response_code = util.get_history(ticker, check_period)

                    if response_code == 200:
                        if payload == "Ticker Not Found":
                            print(f"{ticker}: Not Found")
                        else:
                            transformed_payload = util.transform_payload(payload, ticker, latest_date)
                            message, status_code = util.save_batch_history(transformed_payload)
                            if status_code == 201:
                                print(f"{ticker}: successfully saved - period: {check_period}")
                            else:
                                print(f"{ticker}: Error saving to database")
                                scrape_state.last_error = f"{ticker}: Database save failed"
                    else:
                        print(f"{ticker}: Error scraping YFinance")
                        scrape_state.last_error = f"{ticker}: YFinance scrape failed"

                except Exception as e:
                    print(f"{ticker}: Error - {str(e)}")
                    scrape_state.last_error = f"{ticker}: {str(e)}"

                scrape_state.processed_tickers += 1
                time.sleep(5)  # Rate limiting

    except Exception as e:
        scrape_state.last_error = str(e)
        print(f"Scrape job error: {e}")
    finally:
        scrape_state.is_running = False
        scrape_state.current_ticker = None
        scrape_state.completed_at = time.strftime("%Y-%m-%d %H:%M:%S")
        print("Scrape job completed")


def main():
    print("Scraper Manager CronJob Starting")
    try:
        run_scrape_job()
        if scrape_state.last_error:
            print(f"Scrape completed with errors. Last error: {scrape_state.last_error}")
        print(f"Processed {scrape_state.processed_tickers}/{scrape_state.total_tickers} tickers")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error in scrape job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
