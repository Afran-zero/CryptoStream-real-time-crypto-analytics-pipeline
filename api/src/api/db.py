"""Connection pool lifecycle.

A module-level pool keeps connections warm across requests. The pool is
opened on app startup and closed on shutdown so SIGTERM during a
rolling restart doesn't leak sockets.
"""
from __future__ import annotations

from psycopg_pool import ConnectionPool

from api.config import ApiConfig


def build_pool(cfg: ApiConfig) -> ConnectionPool:
    # `open=False` so the caller controls when the pool is ready (we
    # open in `lifespan` to fail-fast on a misconfigured DB URL).
    return ConnectionPool(
        conninfo=cfg.database_url,
        min_size=cfg.pool_min,
        max_size=cfg.pool_max,
        timeout=cfg.pool_timeout_s,
        open=False,
        kwargs={"autocommit": True},
    )