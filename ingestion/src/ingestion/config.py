"""Configuration loaded from environment variables.

Required env vars (the compose file passes these from `.env`):
    FREECRYPTO_WS_URL    e.g. wss://api.freecryptoapi.com/ws
    FREECRYPTO_API_KEY   source API key
    WATCHLIST            comma-separated symbols, e.g. BTCUSD,ETHUSD,SOLUSD
    KAFKA_BOOTSTRAP      e.g. kafka:9092
    KAFKA_TOPIC_PRICES   default `prices`
    KAFKA_TOPIC_DLQ      default `prices.dlq`

Optional tuning knobs:
    SUBSCRIBE_TIMEOUT_S       default 10
    RECONNECT_INITIAL_S       default 1
    RECONNECT_CAP_S           default 30
    FREECRYPTO_SUBSCRIBE_FMT  `action_symbols` (default) | `type_channels`
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from cryptostream_common import ConfigError, _optional_str, _require


def _optional_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"env var {name!r} is not a number: {raw!r}") from exc


@dataclass(frozen=True)
class Config:
    ws_url: str
    api_key: str
    watchlist: tuple[str, ...]
    kafka_bootstrap: str
    topic_prices: str
    topic_dlq: str
    subscribe_timeout_s: float
    reconnect_initial_s: float
    reconnect_cap_s: float
    subscribe_format: str  # `action_symbols` or `type_channels`

    @classmethod
    def from_env(cls) -> "Config":
        watchlist_raw = _require("WATCHLIST")
        symbols = tuple(s.strip() for s in watchlist_raw.split(",") if s.strip())
        if not symbols:
            raise ConfigError("WATCHLIST env var is empty after parsing")

        subscribe_format = _optional_str("FREECRYPTO_SUBSCRIBE_FMT", "action_symbols")
        if subscribe_format not in {"action_symbols", "type_channels"}:
            raise ConfigError(
                f"FREECRYPTO_SUBSCRIBE_FMT must be one of "
                f"'action_symbols' or 'type_channels', got {subscribe_format!r}"
            )

        return cls(
            ws_url=_require("FREECRYPTO_WS_URL"),
            api_key=_require("FREECRYPTO_API_KEY"),
            watchlist=symbols,
            kafka_bootstrap=_require("KAFKA_BOOTSTRAP"),
            topic_prices=_optional_str("KAFKA_TOPIC_PRICES", "prices"),
            topic_dlq=_optional_str("KAFKA_TOPIC_DLQ", "prices.dlq"),
            subscribe_timeout_s=_optional_float("SUBSCRIBE_TIMEOUT_S", 10.0),
            reconnect_initial_s=_optional_float("RECONNECT_INITIAL_S", 1.0),
            reconnect_cap_s=_optional_float("RECONNECT_CAP_S", 30.0),
            subscribe_format=subscribe_format,
        )