"""Tests for service credentials on database-service requests."""

from datetime import date

import pytest

from scraper_manager.config import Config
from scraper_manager.http_client import HTTPClient

TOKEN = "t" * 40


class FakeResponse:
    status = 200
    headers = {}
    history = ()
    request_info = None

    def __init__(self, payload=None):
        self._payload = payload if payload is not None else {}

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload

    async def text(self):
        return ""

    def release(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = []

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return FakeResponse([] if url.endswith("/tickers/stale") else {"ok": True})

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return FakeResponse({"id": "run-1"})


def make_client():
    config = Config()
    config.services.database_service_url = "http://db.test"
    config.services.yfinance_service_url = "http://yf.test"
    config.services.database_service_token = TOKEN
    client = HTTPClient(config)
    client._session = FakeSession()
    return client


def auth_header(call):
    return (call[2].get("headers") or {}).get("Authorization")


async def test_database_requests_send_bearer_token():
    client = make_client()
    await client.get_stale_tickers(date(2026, 10, 26), 0, 10)
    await client.start_scraper_run("run-key", date(2026, 10, 26), shadow_mode=False)
    await client.update_scraper_run_progress("run-1", processed_delta=1)
    await client.complete_scraper_run("run-1", "completed")
    await client.save_batch([{"ticker_id": 1}])

    calls = client._session.calls
    assert [call[1] for call in calls] == [
        "http://db.test/tickers/stale",
        "http://db.test/scraper-runs/start",
        "http://db.test/scraper-runs/run-1/progress",
        "http://db.test/scraper-runs/run-1/complete",
        "http://db.test/history/batch",
    ]
    assert all(auth_header(call) == f"Bearer {TOKEN}" for call in calls)


async def test_yfinance_requests_never_send_token():
    client = make_client()
    await client.fetch_yfinance("AAPL", period="5d")

    (call,) = client._session.calls
    assert call[1] == "http://yf.test/history"
    assert auth_header(call) is None


async def test_no_header_without_token():
    client = make_client()
    client.config.services.database_service_token = ""
    await client.complete_scraper_run("run-1", "completed")

    (call,) = client._session.calls
    assert auth_header(call) is None


@pytest.mark.parametrize("shadow_mode", [True, False])
def test_token_not_in_config_repr(shadow_mode):
    config = Config()
    config.runtime.shadow_mode = shadow_mode
    config.services.database_service_token = TOKEN
    assert TOKEN not in repr(config)
