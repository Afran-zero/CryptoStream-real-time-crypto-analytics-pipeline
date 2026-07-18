"""Pure, import-safe normalizer for incoming WebSocket payloads.

The FreeCryptoAPI free-tier wire format is unresolved (PRD §13 OQ1), so the
normalizer tries a set of common field aliases. On a hit, it builds a
canonical candidate and runs it through the Pydantic v2 model
`CanonicalTick`. Output is the exact Module 0 §5 schema; failures raise
`ValidationError` so the caller can route them to the DLQ.

Import-safe: no network, no Kafka, no logging at import time.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, MutableMapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError, field_validator


class ValidationError(Exception):
    """Raised when a raw payload cannot be mapped to the canonical schema."""


class CanonicalTick(BaseModel):
    """Module 0 §5 canonical contract."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    price: Decimal
    volume: Decimal | None = None
    event_time: datetime
    ingested_at: datetime
    source: str = Field(min_length=1)

    @field_validator("price")
    @classmethod
    def _price_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("price must be > 0")
        return v

    @field_validator("event_time", "ingested_at")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        # FreeCryptoAPI delivers UTC; coerce naive timestamps to UTC.
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


# Aliases tried in order for each canonical field. The first match wins.
SYMBOL_ALIASES: tuple[str, ...] = ("symbol", "pair", "s", "ticker", "instrument")
EXCHANGE_ALIASES: tuple[str, ...] = ("exchange", "ex", "source", "venue", "market")
PRICE_ALIASES: tuple[str, ...] = ("price", "p", "last", "lastPrice", "last_price", "close")
VOLUME_ALIASES: tuple[str, ...] = ("volume", "v", "qty", "quantity", "baseVolume", "base_volume")
EVENT_TIME_ALIASES: tuple[str, ...] = (
    "event_time", "eventTime", "timestamp", "time", "t", "ts", "date",
)
SOURCE_ALIASES: tuple[str, ...] = ("source", "src", "feed", "provider")


def _first_present(raw: Mapping[str, Any], keys: Iterable[str]) -> Any:
    """Return the first value found under any of the given keys (case-insensitive).

    Hot path: try exact-case keys first, then fall back to a single lower_map
    lookup only if none matched. Avoids the dict allocation on the common path.
    """
    for k in keys:
        if k in raw:
            return raw[k]
    lower_map = {str(k).lower(): v for k, v in raw.items()}
    for k in keys:
        if k.lower() in lower_map:
            return lower_map[k.lower()]
    return None


def _coerce_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        try:
            return Decimal(str(value))
        except Exception as exc:  # noqa: BLE001 — want to wrap any parsing failure
            raise ValidationError(f"cannot parse {value!r} as decimal") from exc
    raise ValidationError(f"unsupported decimal type: {type(value).__name__}")


def _coerce_datetime(value: Any) -> datetime:
    """Tolerate epoch ms / s / ISO-8601 / datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Heuristic: > 10^12 means ms, otherwise seconds.
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise ValidationError("empty timestamp string")
        # Accept trailing `Z` (RFC 3339 / ISO 8601 UTC).
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as exc:
            raise ValidationError(f"cannot parse timestamp {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    raise ValidationError(f"unsupported timestamp type: {type(value).__name__}")


def normalize(raw: Mapping[str, Any] | None) -> dict:
    """Map a raw WebSocket payload to the canonical Module 0 §5 dict.

    Raises `ValidationError` if any required field is missing or malformed.
    """
    if not isinstance(raw, Mapping):
        raise ValidationError(f"payload is not a mapping: {type(raw).__name__}")

    symbol = _first_present(raw, SYMBOL_ALIASES)
    exchange = _first_present(raw, EXCHANGE_ALIASES)
    price_raw = _first_present(raw, PRICE_ALIASES)
    volume_raw = _first_present(raw, VOLUME_ALIASES)
    event_raw = _first_present(raw, EVENT_TIME_ALIASES)
    source_raw = _first_present(raw, SOURCE_ALIASES)

    missing = [
        name
        for name, value in (
            ("symbol", symbol),
            ("exchange", exchange),
            ("price", price_raw),
            ("event_time", event_raw),
        )
        if value is None
    ]
    if missing:
        raise ValidationError(f"missing fields: {', '.join(missing)}")

    # Build the candidate mapping; Pydantic validates the rest.
    candidate: MutableMapping[str, Any] = {
        "symbol": str(symbol).upper().strip(),
        "exchange": str(exchange).lower().strip(),
        "price": _coerce_decimal(price_raw),
        "volume": _coerce_decimal(volume_raw) if volume_raw is not None else None,
        "event_time": _coerce_datetime(event_raw),
        "ingested_at": datetime.now(tz=timezone.utc),
        # Treat anything that isn't literally `freecryptoapi` as foreign;
        # we don't trust other "source" values from upstream.
        "source": "freecryptoapi",
    }
    if source_raw is not None and str(source_raw).lower() not in {
        "freecryptoapi",
        "freecrypto",
        "free_crypto_api",
    }:
        raise ValidationError(
            f"untrusted source field {source_raw!r}; expected 'freecryptoapi'"
        )

    try:
        tick = CanonicalTick.model_validate(candidate)
    except PydanticValidationError as exc:
        # Flatten to a single human-readable line for the DLQ.
        first = exc.errors()[0] if exc.errors() else {}
        loc = ".".join(str(p) for p in first.get("loc", ())) or "?"
        msg = first.get("msg", "validation failed")
        raise ValidationError(f"{loc}: {msg}") from exc

    # JSON-ready dict: decimals as strings, timestamps ISO-8601 UTC with `Z`.
    out = tick.model_dump()
    out["price"] = str(out["price"])
    if out["volume"] is not None:
        out["volume"] = str(out["volume"])
    for ts_field in ("event_time", "ingested_at"):
        dt = getattr(tick, ts_field)
        out[ts_field] = (
            dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
        )
    return out