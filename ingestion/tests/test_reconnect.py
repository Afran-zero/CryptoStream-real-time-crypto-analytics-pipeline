"""Unit tests for the reconnect backoff schedule."""
from __future__ import annotations

import random

from ingestion.config import Config
from ingestion.ws_client import WebSocketIngestor


def _config(**overrides) -> Config:
    base = dict(
        ws_url="ws://test",
        api_key="test",
        watchlist=("BTCUSD",),
        kafka_bootstrap="kafka:9092",
        topic_prices="prices",
        topic_dlq="prices.dlq",
        subscribe_timeout_s=1.0,
        reconnect_initial_s=1.0,
        reconnect_cap_s=30.0,
        subscribe_format="action_symbols",
    )
    base.update(overrides)
    return Config(**base)


def test_backoff_doubles_until_cap(monkeypatch):
    """Without jitter, schedule is 1, 2, 4, 8, 16, 30 (cap), 30, ..."""
    # Pin random.uniform to zero so test is deterministic.
    monkeypatch.setattr(random, "uniform", lambda _a, _b: 0.0)
    ingestor = WebSocketIngestor(config=_config(), publisher=None)  # type: ignore[arg-type]

    assert ingestor.compute_backoff(1) == 1.0
    assert ingestor.compute_backoff(2) == 2.0
    assert ingestor.compute_backoff(3) == 4.0
    assert ingestor.compute_backoff(4) == 8.0
    assert ingestor.compute_backoff(5) == 16.0
    # Capped at 30 for attempt 6 onward.
    for n in range(6, 12):
        assert ingestor.compute_backoff(n) == 30.0


def test_backoff_includes_jitter(monkeypatch):
    """Verify jitter adds a bounded amount."""
    monkeypatch.setattr(random, "uniform", lambda _a, b: b)
    ingestor = WebSocketIngestor(config=_config(reconnect_cap_s=30.0), publisher=None)  # type: ignore[arg-type]
    # With jitter at the max, attempt 5 (16s base) + min(0.5, 1.6) = 16.5
    val = ingestor.compute_backoff(5)
    assert 16.0 < val <= 16.5


def test_backoff_respects_custom_initial_and_cap(monkeypatch):
    monkeypatch.setattr(random, "uniform", lambda _a, _b: 0.0)
    ingestor = WebSocketIngestor(
        config=_config(reconnect_initial_s=0.5, reconnect_cap_s=2.0),
        publisher=None,  # type: ignore[arg-type]
    )
    assert ingestor.compute_backoff(1) == 0.5
    assert ingestor.compute_backoff(2) == 1.0
    assert ingestor.compute_backoff(3) == 2.0  # cap
    assert ingestor.compute_backoff(10) == 2.0


def test_subscribe_frame_default_shape():
    ingestor = WebSocketIngestor(
        config=_config(watchlist=("BTCUSD", "ETHUSD")),
        publisher=None,  # type: ignore[arg-type]
    )
    frame = ingestor.build_subscribe_frame()
    assert frame == {"action": "subscribe", "symbols": ["BTCUSD", "ETHUSD"]}


def test_subscribe_frame_type_channels_format():
    ingestor = WebSocketIngestor(
        config=_config(subscribe_format="type_channels", watchlist=("BTCUSD",)),
        publisher=None,  # type: ignore[arg-type]
    )
    frame = ingestor.build_subscribe_frame()
    assert frame == {"type": "subscribe", "channels": ["BTCUSD"]}
