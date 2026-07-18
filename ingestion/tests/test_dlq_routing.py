"""Unit tests for ingestion.ws_client.DLQ routing."""
from __future__ import annotations

import pytest

from ingestion.config import Config
from ingestion.producer import FakePublisher
from ingestion.ws_client import WebSocketIngestor


def _config(**overrides) -> Config:
    base = dict(
        ws_url="ws://test",
        api_key="test",
        watchlist=("BTCUSD", "ETHUSD"),
        kafka_bootstrap="kafka:9092",
        topic_prices="prices",
        topic_dlq="prices.dlq",
        subscribe_timeout_s=1.0,
        reconnect_initial_s=0.1,
        reconnect_cap_s=1.0,
        subscribe_format="action_symbols",
    )
    base.update(overrides)
    return Config(**base)


@pytest.mark.asyncio
async def test_mixed_batch_routes_correctly(fake_publisher: FakePublisher):
    ingestor = WebSocketIngestor(config=_config(), publisher=fake_publisher)

    messages = [
        # 1. canonical good
        {
            "symbol": "BTCUSD",
            "exchange": "binance",
            "price": 68000.12,
            "volume": 1.0,
            "event_time": "2026-06-09T12:00:00.000Z",
        },
        # 2. bad: missing event_time
        {"symbol": "ETHUSD", "exchange": "kraken", "price": 3500.0},
        # 3. bad: zero price
        {
            "symbol": "SOLUSD",
            "exchange": "coinbase",
            "price": 0,
            "event_time": "2026-06-09T12:00:00.000Z",
        },
        # 4. bad: garbage (no recognizable fields)
        {"wat": "lol"},
        # 5. raw string with malformed JSON — routed to DLQ
        "not json {{",
        # 6. raw bytes with malformed JSON
        b"\xff\xfe\x00",
    ]

    for m in messages:
        await ingestor.handle_message(m)

    prices = fake_publisher.by_topic("prices")
    dlq = fake_publisher.by_topic("prices.dlq")

    assert len(prices) == 1
    assert prices[0][0] == "BTCUSD"  # keyed by symbol

    # 5 messages should land in DLQ: #2, #3, #4, #5, #6
    assert len(dlq) == 5

    # Counters reflect the routing
    assert ingestor.counters.published == 1
    assert ingestor.counters.dlq == 5
    assert ingestor.counters.received == 6

    # Loop never raised
    assert True


@pytest.mark.asyncio
async def test_dlq_record_shape(fake_publisher: FakePublisher):
    ingestor = WebSocketIngestor(config=_config(), publisher=fake_publisher)
    await ingestor.handle_message({"foo": "bar"})
    dlq = fake_publisher.by_topic("prices.dlq")
    assert len(dlq) == 1
    _key, record = dlq[0]
    assert "error" in record
    assert "payload" in record
    assert "dlq_at" in record
    assert record["dlq_at"].endswith("Z")


@pytest.mark.asyncio
async def test_bytes_payload_with_valid_json_publishes(fake_publisher: FakePublisher):
    import orjson

    ingestor = WebSocketIngestor(config=_config(), publisher=fake_publisher)
    good = orjson.dumps(
        {
            "symbol": "BTCUSD",
            "exchange": "binance",
            "price": 100,
            "event_time": "2026-06-09T12:00:00Z",
        }
    )
    await ingestor.handle_message(good)
    assert ingestor.counters.published == 1
    assert fake_publisher.by_topic("prices")[0][0] == "BTCUSD"