"""Endpoint contract tests via FastAPI's TestClient."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from api.config import ApiConfig
from api.main import app


@pytest.fixture
def patched_app(pg_url, gold_schema):
    """Wire `app.state` to point at the test schema; restore on teardown.

    Mutating module-level `app.state` means tests can interleave if
    run in parallel, so we save and restore the original config/pool.
    """
    cfg = ApiConfig(
        database_url=pg_url,
        gold_schema=gold_schema,
        cors_origins=("http://localhost",),
        pool_min=1,
        pool_max=2,
        pool_timeout_s=10,
    )

    pool = ConnectionPool(
        conninfo=pg_url,
        min_size=1,
        max_size=2,
        timeout=10,
        kwargs={"autocommit": True},
    )
    pool.open(wait=True, timeout=10)

    saved_cfg = getattr(app.state, "config", None)
    saved_pool = getattr(app.state, "pool", None)
    app.state.config = cfg
    app.state.pool = pool
    try:
        yield TestClient(app)
    finally:
        pool.close()
        if saved_cfg is not None:
            app.state.config = saved_cfg
        if saved_pool is not None:
            app.state.pool = saved_pool


def test_health_endpoint_returns_ok(patched_app):
    resp = patched_app.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["db"] == "ok"
    assert isinstance(body["gold_freshness_seconds"], int)


def test_prices_latest_endpoint_returns_seeded_row(patched_app):
    resp = patched_app.get("/prices/latest?symbols=BTCUSD")
    assert resp.status_code == 200
    body = resp.json()
    assert "prices" in body
    assert any(p["symbol"] == "BTCUSD" for p in body["prices"])


def test_prices_latest_unknown_symbol_returns_empty_list(patched_app):
    resp = patched_app.get("/prices/latest?symbols=DOESNOTEXIST")
    assert resp.status_code == 200
    body = resp.json()
    assert body["prices"] == []


def test_prices_latest_missing_query_param_returns_422(patched_app):
    resp = patched_app.get("/prices/latest")
    assert resp.status_code == 422


def test_candles_endpoint_returns_ohlc(patched_app):
    resp = patched_app.get("/candles/BTCUSD?interval=1m&limit=3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "BTCUSD"
    assert body["interval"] == "1m"
    assert len(body["candles"]) == 3
    sample = body["candles"][0]
    for k in ("open", "high", "low", "close", "volume"):
        assert k in sample


def test_candles_unknown_symbol_returns_empty_list(patched_app):
    resp = patched_app.get("/candles/MISSING?interval=1m&limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["candles"] == []


def test_candles_interval_validation(patched_app):
    resp = patched_app.get("/candles/BTCUSD?interval=5m&limit=10")
    assert resp.status_code == 422


def test_moving_average_endpoint(patched_app):
    resp = patched_app.get("/indicators/BTCUSD/ma?limit=5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "BTCUSD"
    assert body["window"] == 20
    assert len(body["points"]) == 5
    assert all("ma_20" in p for p in body["points"])


def test_openapi_docs_available(patched_app):
    assert patched_app.get("/openapi.json").status_code == 200
    assert patched_app.get("/docs").status_code == 200