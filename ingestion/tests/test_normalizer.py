"""Unit tests for ingestion.normalizer."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.normalizer import ValidationError, normalize


def _valid_payload() -> dict:
    return {
        "symbol": "BTCUSD",
        "exchange": "binance",
        "price": 68000.12,
        "volume": 1.234,
        "event_time": "2026-06-09T12:00:00.000Z",
    }


def test_canonical_aliases_produce_module_0_schema():
    tick = normalize(_valid_payload())
    assert tick["symbol"] == "BTCUSD"
    assert tick["exchange"] == "binance"
    assert tick["source"] == "freecryptoapi"
    # price and volume serialised as strings, event_time as ISO-Z
    assert tick["price"] == "68000.12"
    assert tick["volume"] == "1.234"
    assert tick["event_time"].endswith("Z")
    # ingested_at is a recent UTC ISO string with Z
    assert tick["ingested_at"].endswith("Z")
    parsed = datetime.fromisoformat(tick["ingested_at"].replace("Z", "+00:00"))
    assert parsed.tzinfo is not None


def test_pair_aliases_for_symbol_and_p_for_price():
    payload = {
        "pair": "ethusd",
        "ex": "kraken",
        "p": "3450.55",
        "v": "0.5",
        "timestamp": 1717934400,  # epoch seconds
    }
    tick = normalize(payload)
    assert tick["symbol"] == "ETHUSD"  # uppercased
    assert tick["exchange"] == "kraken"
    assert tick["price"] == "3450.55"
    assert tick["volume"] == "0.5"
    assert tick["event_time"].startswith("2024-")  # 2024-06-09 from that epoch


def test_epoch_milliseconds_detected():
    payload = {
        "s": "BTCUSD",
        "exchange": "binance",
        "price": 100,
        "t": 1717934400000,  # epoch ms (> 1e12)
    }
    tick = normalize(payload)
    assert tick["event_time"].startswith("2024-06-09T")


@pytest.mark.parametrize(
    "field,mutator",
    [
        ("symbol", lambda p: p.pop("symbol")),
        ("exchange", lambda p: p.pop("exchange")),
        ("price", lambda p: p.pop("price")),
        ("event_time", lambda p: p.pop("event_time")),
    ],
)
def test_missing_required_fields_raise(field, mutator):
    payload = _valid_payload()
    mutator(payload)
    with pytest.raises(ValidationError) as ei:
        normalize(payload)
    assert field in str(ei.value) or "missing" in str(ei.value).lower()


def test_negative_price_rejected():
    payload = _valid_payload() | {"price": -1}
    with pytest.raises(ValidationError):
        normalize(payload)


def test_zero_price_rejected():
    payload = _valid_payload() | {"price": 0}
    with pytest.raises(ValidationError):
        normalize(payload)


def test_unparseable_timestamp_rejected():
    payload = _valid_payload() | {"event_time": "not-a-date"}
    with pytest.raises(ValidationError):
        normalize(payload)


def test_empty_exchange_rejected():
    payload = _valid_payload() | {"exchange": ""}
    with pytest.raises(ValidationError):
        normalize(payload)


def test_unknown_payload_with_no_recognizable_fields_rejected():
    with pytest.raises(ValidationError):
        normalize({"foo": "bar", "baz": 42})


def test_untrusted_source_field_rejected():
    payload = _valid_payload() | {"source": "shadyfeed"}
    with pytest.raises(ValidationError) as ei:
        normalize(payload)
    assert "source" in str(ei.value).lower()


def test_volume_optional():
    payload = _valid_payload()
    payload.pop("volume")
    tick = normalize(payload)
    assert tick["volume"] is None
