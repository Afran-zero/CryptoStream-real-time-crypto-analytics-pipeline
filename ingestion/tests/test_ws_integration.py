"""Integration tests — run the real WebSocket loop against a local server.

Skipped by `pytest -q`. Run via:
    docker compose run --rm ingestion pytest -q -m integration
or:
    pytest -m integration
"""
from __future__ import annotations

import asyncio
import json

import pytest

from ingestion.config import Config
from ingestion.producer import FakePublisher
from ingestion.ws_client import WebSocketIngestor


pytestmark = pytest.mark.integration


def _config(ws_url: str) -> Config:
    return Config(
        ws_url=ws_url,
        api_key="test",
        watchlist=("BTCUSD", "ETHUSD"),
        kafka_bootstrap="kafka:9092",
        topic_prices="prices",
        topic_dlq="prices.dlq",
        subscribe_timeout_s=2.0,
        reconnect_initial_s=0.1,
        reconnect_cap_s=0.3,
        subscribe_format="action_symbols",
    )


def _good_msg(symbol: str = "BTCUSD", price: float = 100.0) -> dict:
    return {
        "symbol": symbol,
        "exchange": "binance",
        "price": price,
        "volume": 1.0,
        "event_time": "2026-06-09T12:00:00Z",
    }


async def _echo_handler_with_script(script: list, ws):
    """Accept subscribe, then push every item in `script`, ignoring inbound."""
    async for msg in ws:  # type: ignore[attr-defined]
        try:
            data = json.loads(msg)
        except Exception:
            continue
        if data.get("action") == "subscribe":
            for item in script:
                await ws.send(json.dumps(item))


@pytest.mark.asyncio
async def test_loop_consumes_real_ws_messages():
    """A local websockets server pushes valid + invalid frames;
    the ingestor routes them via FakePublisher."""
    import websockets

    script = [
        _good_msg("BTCUSD", 100.0),
        _good_msg("ETHUSD", 200.0),
        {"not": "valid"},   # -> DLQ
        "literally: not json",  # -> DLQ
        _good_msg("BTCUSD", 105.0),
    ]

    server = await websockets.serve(
        lambda ws: _echo_handler_with_script(script, ws),
        "127.0.0.1",
        0,
    )
    try:
        # Pick the actual port the server bound to.
        host, port = server.sockets[0].getsockname()[:2]
        ws_url = f"ws://{host}:{port}"

        publisher = FakePublisher()
        ingestor = WebSocketIngestor(config=_config(ws_url), publisher=publisher)

        # Run for a bounded number of messages or short timeout.
        run_task = asyncio.create_task(ingestor.run())
        try:
            await asyncio.wait_for(run_task, timeout=2.0)
        except asyncio.TimeoutError:
            run_task.cancel()
            try:
                await run_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        finally:
            # Best-effort: close the WS connection by sending shutdown.
            pass

        prices = publisher.by_topic("prices")
        dlq = publisher.by_topic("prices.dlq")

        # 3 valid ticks (BTCUSD x2, ETHUSD x1), 2 DLQ
        assert len(prices) >= 3
        assert len(dlq) >= 2
        assert ingestor.counters.published >= 3
        assert ingestor.counters.dlq >= 2
        # Loop must have at least one successful subscribe.
        assert ingestor.counters.subscribed >= 1
    finally:
        server.close()
        await server.wait_closed()