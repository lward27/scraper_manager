"""Message schema used between scheduler and worker."""

import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone


@dataclass
class TickerTaskMessage:
    run_id: str
    ticker_id: int
    ticker: str
    last_date: str
    target_date: str
    scheduled_for: str
    attempt: int = 0
    enqueued_at: str = ""

    def __post_init__(self):
        if not self.enqueued_at:
            self.enqueued_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_dict(cls, payload: dict) -> "TickerTaskMessage":
        required = {
            "run_id",
            "ticker_id",
            "ticker",
            "last_date",
            "target_date",
            "scheduled_for",
            "attempt",
            "enqueued_at",
        }
        missing = sorted(required - payload.keys())
        if missing:
            raise ValueError(f"Missing fields in task payload: {missing}")

        return cls(
            run_id=str(payload["run_id"]),
            ticker_id=int(payload["ticker_id"]),
            ticker=str(payload["ticker"]),
            last_date=str(payload["last_date"]),
            target_date=str(payload["target_date"]),
            scheduled_for=str(payload["scheduled_for"]),
            attempt=int(payload["attempt"]),
            enqueued_at=str(payload["enqueued_at"]),
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> "TickerTaskMessage":
        return cls.from_dict(json.loads(payload.decode("utf-8")))

    def next_attempt(self) -> "TickerTaskMessage":
        return replace(
            self,
            attempt=self.attempt + 1,
            enqueued_at=datetime.now(timezone.utc).isoformat(),
        )
