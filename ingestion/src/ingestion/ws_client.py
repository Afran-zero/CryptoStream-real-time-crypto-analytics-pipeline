"""Async WebSocket ingestor.

Implements:
- `WebSocketIngestor.handle_message(raw)` — public method, parses one
  message, publishes to Kafka or DLQ. Used both by `run()` and by tests.
- `WebSocketIngestor.run()` — main async loop: connect, subscribe, read,
  route, reconnect with exponential backoff + jitter.

The reconnect loop is robust to dropped sockets and bad credentials: a
single bad message never breaks the loop, and the backoff schedule
resets on a successful subscribe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import orjson
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from .config import Config
from .normalizer import ValidationError, normalize
from .producer import Publisher

logger = logging.getLogger(__name__)


def _iso_utc_ms(dt: datetime) -> str:
    """RFC-3339 / ISO-8601 UTC with millisecond precision and `Z`."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


@dataclass
class IngestCounters:
    published: int = 0
    dlq: int = 0
    reconnects: int = 0
    subscribed: int = 0
    received: int = 0


@dataclass
class WebSocketIngestor:
    config: Config
    publisher: Publisher
    # Test hook: lets unit tests swap in a deterministic sleeper.
    sleep: Callable[[float], Any] = field(default=asyncio.sleep)
    counters: IngestCounters = field(default_factory=IngestCounters)

    # ---------- pure helpers (testable) ----------

    def build_subscribe_frame(self) -> dict:
        """Return the subscribe frame to send immediately after connect.

        Two common shapes supported via `FREECRYPTO_SUBSCRIBE_FMT`:
          - action_symbols : `{"action":"subscribe","symbols":["BTCUSD",...]}`
          - type_channels  : `{"type":"subscribe","channels":["BTCUSD",...]}`
        """
        symbols = list(self.config.watchlist)
        if self.config.subscribe_format == "type_channels":
            return {"type": "subscribe", "channels": symbols}
        return {"action": "subscribe", "symbols": symbols}

    def compute_backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter, capped at reconnect_cap_s.

        `attempt` is the count of consecutive failed reconnects since the
        last successful subscribe. Caller passes `attempt >= 1`.
        """
        base = self.config.reconnect_initial_s * (2 ** (attempt - 1))
        capped = min(self.config.reconnect_cap_s, base)
        jitter = random.uniform(0.0, min(0.5, capped * 0.1))
        return capped + jitter

    # ---------- message processing ----------

    async def handle_message(self, raw: bytes | str | dict) -> None:
        """Parse one message and route it to prices or DLQ. Never raises."""
        self.counters.received += 1
        if isinstance(raw, (bytes, bytearray)):
            try:
                payload = orjson.loads(raw)
            except orjson.JSONDecodeError:
                self._publish_dlq(raw, error="invalid json (bytes)")
                return
            self._route(payload)
            return
        if isinstance(raw, str):
            # orjson.loads raises on str; go straight to json.loads.
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._publish_dlq(raw, error="invalid json (str)")
                return
            self._route(payload)
            return
        if isinstance(raw, dict):
            self._route(raw)
            return
        self._publish_dlq(raw, error=f"unsupported message type {type(raw).__name__}")

    def _route(self, payload: Any) -> None:
        try:
            tick = normalize(payload)
        except ValidationError as exc:
            self._publish_dlq(payload, error=str(exc))
            return
        self.publisher.publish(
            self.config.topic_prices,
            key=tick["symbol"],
            value=tick,
        )
        self.counters.published += 1

    def _publish_dlq(self, payload: Any, *, error: str) -> None:
        # `payload` is passed as-is; the publisher's `orjson.dumps(default=str)`
        # handles any non-JSON-native types so we don't need a recursive helper.
        record = {
            "error": error,
            "payload": payload,
            "dlq_at": _iso_utc_ms(datetime.now(tz=timezone.utc)),
        }
        key = "unknown"
        if isinstance(payload, dict):
            sym = payload.get("symbol") or payload.get("pair") or payload.get("s")
            if sym:
                key = str(sym)
        self.publisher.publish(self.config.topic_dlq, key=key, value=record)
        self.counters.dlq += 1

    # ---------- main async loop ----------

    async def run(self) -> None:
        """Connect, subscribe, read messages forever; reconnect on failure."""
        attempt = 0
        while True:
            try:
                async with websockets.connect(
                    self.config.ws_url,
                    additional_headers={
                        "Authorization": f"Bearer {self.config.api_key}",
                    },
                    open_timeout=self.config.subscribe_timeout_s,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(orjson.dumps(self.build_subscribe_frame()))
                    logger.info(
                        "ws.connected",
                        extra={
                            "ws_url": self.config.ws_url,
                            "watchlist": list(self.config.watchlist),
                        },
                    )
                    self.counters.subscribed += 1
                    attempt = 0  # reset backoff on successful subscribe
                    async for msg in ws:
                        await self.handle_message(msg)
            except (ConnectionClosed, InvalidStatus, WebSocketException, OSError, asyncio.TimeoutError) as exc:
                attempt += 1
                delay = self.compute_backoff(attempt)
                self.counters.reconnects += 1
                logger.warning(
                    "ws.reconnect",
                    extra={
                        "attempt": attempt,
                        "delay_s": round(delay, 2),
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                )
                await self.sleep(delay)