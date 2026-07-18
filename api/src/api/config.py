"""API runtime configuration."""
from __future__ import annotations

from dataclasses import dataclass

from cryptostream_common import _optional_str, _require

DEFAULT_DATABASE_URL = "postgresql://cryptostream:cryptostream@postgres:5432/cryptostream"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_CORS_ORIGINS = "http://localhost:5173,http://localhost:8000"
DEFAULT_POOL_MIN = 1
DEFAULT_POOL_MAX = 8
DEFAULT_POOL_TIMEOUT_S = 10


@dataclass(frozen=True)
class ApiConfig:
    database_url: str
    gold_schema: str
    cors_origins: tuple[str, ...]
    pool_min: int
    pool_max: int
    pool_timeout_s: int

    @classmethod
    def from_env(cls) -> "ApiConfig":
        return cls(
            database_url=_require("DATABASE_URL"),
            gold_schema=_optional_str("GOLD_SCHEMA", DEFAULT_GOLD_SCHEMA),
            cors_origins=tuple(
                o.strip() for o in _optional_str("CORS_ORIGINS", DEFAULT_CORS_ORIGINS).split(",") if o.strip()
            ),
            pool_min=int(_optional_str("DB_POOL_MIN", str(DEFAULT_POOL_MIN))),
            pool_max=int(_optional_str("DB_POOL_MAX", str(DEFAULT_POOL_MAX))),
            pool_timeout_s=int(_optional_str("DB_POOL_TIMEOUT_S", str(DEFAULT_POOL_TIMEOUT_S))),
        )