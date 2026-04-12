from scraper_manager.messages import TickerTaskMessage


def test_message_round_trip():
    msg = TickerTaskMessage(
        run_id="run-1",
        ticker_id=42,
        ticker="AAPL",
        last_date="2026-04-10",
        target_date="2026-04-11",
        scheduled_for="2026-04-11",
        attempt=0,
    )

    decoded = TickerTaskMessage.from_bytes(msg.to_bytes())

    assert decoded.run_id == "run-1"
    assert decoded.ticker_id == 42
    assert decoded.ticker == "AAPL"
    assert decoded.attempt == 0


def test_next_attempt_increments():
    msg = TickerTaskMessage(
        run_id="run-1",
        ticker_id=1,
        ticker="MSFT",
        last_date="2026-04-01",
        target_date="2026-04-10",
        scheduled_for="2026-04-10",
        attempt=2,
    )

    next_msg = msg.next_attempt()

    assert next_msg.attempt == 3
    assert next_msg.enqueued_at != msg.enqueued_at
