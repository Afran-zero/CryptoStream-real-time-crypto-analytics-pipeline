"""Repository unit tests against a seeded temp schema."""
from __future__ import annotations

import psycopg

from api.repository import GoldRepository


def test_health_reports_db_ok(pg_url, gold_schema):
    with psycopg.connect(pg_url) as conn:
        repo = GoldRepository(conn, gold_schema=gold_schema)
        status = repo.health()
    assert status.db_ok is True
    # Freshness should be a small positive number (we just seeded).
    assert status.freshness_seconds is not None
    assert status.freshness_seconds >= 0


def test_latest_prices_returns_most_recent_per_exchange(pg_url, gold_schema):
    with psycopg.connect(pg_url) as conn:
        repo = GoldRepository(conn, gold_schema=gold_schema)
        rows = repo.latest_prices(["BTCUSD"])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSD"
    # Latest seeded close = 104 (the 5th minute).
    assert float(rows[0]["close"]) == 104.0


def test_latest_prices_unknown_symbol_returns_empty(pg_url, gold_schema):
    with psycopg.connect(pg_url) as conn:
        repo = GoldRepository(conn, gold_schema=gold_schema)
        rows = repo.latest_prices(["DOESNOTEXIST"])
    assert rows == []


def test_candles_ascending_with_limit(pg_url, gold_schema):
    with psycopg.connect(pg_url) as conn:
        repo = GoldRepository(conn, gold_schema=gold_schema)
        rows = repo.candles("BTCUSD", limit=3)
    assert len(rows) == 3
    # Ascending by bucket.
    buckets = [r["bucket"] for r in rows]
    assert buckets == sorted(buckets)
    # First candle's close is the first seeded value (100).
    assert float(rows[0]["close"]) == 100.0


def test_moving_average_returns_rows(pg_url, gold_schema):
    with psycopg.connect(pg_url) as conn:
        repo = GoldRepository(conn, gold_schema=gold_schema)
        rows = repo.moving_average("BTCUSD", limit=5)
    assert len(rows) == 5
    assert all(r["ma_20"] is not None for r in rows)


def test_health_on_empty_schema(pg_url):
    # Create an empty (no tables) schema and verify health returns
    # "no freshness" rather than crashing.
    schema = "api_test_empty"
    with psycopg.connect(pg_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop schema if exists {schema} cascade")
            cur.execute(f"create schema {schema}")
    try:
        with psycopg.connect(pg_url) as conn:
            repo = GoldRepository(conn, gold_schema=schema)
            status = repo.health()
        assert status.db_ok is True
        assert status.freshness_seconds is None
    finally:
        with psycopg.connect(pg_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"drop schema if exists {schema} cascade")
