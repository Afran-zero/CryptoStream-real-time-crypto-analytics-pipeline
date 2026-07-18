"""FastAPI dependencies — request-scoped repository."""
from __future__ import annotations

from fastapi import Request

from api.config import ApiConfig
from api.repository import GoldRepository


def get_config(request: Request) -> ApiConfig:
    return request.app.state.config


def get_repo(request: Request) -> GoldRepository:
    """Yield a repository bound to the request's pooled connection.

    `pool.connection()` is a context manager; FastAPI waits for this
    generator to finish before exiting the `with` block, so the
    connection is returned to the pool only after the response is
    serialized.
    """
    cfg: ApiConfig = request.app.state.config
    pool = request.app.state.pool
    with pool.connection() as conn:
        yield GoldRepository(conn, gold_schema=cfg.gold_schema)


__all__ = ["get_config", "get_repo"]