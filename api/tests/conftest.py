"""Pytest fixtures for the API tests.

Integration tests require a live Postgres reachable via DATABASE_URL;
they're skipped automatically if it's not set.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pytest


def _requires_postgres() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set; skipping integration test")
    return url


@pytest.fixture
def pg_url() -> str:
    return _requires_postgres()


@pytest.fixture
def gold_schema(pg_url):
    """Create a fresh test schema mirroring the Gold contract and seed it.

    Yields the schema name. Cleans up on teardown.
    """
    schema = "api_test_gold"
    full_candles = f"{schema}.candles_1m"
    full_ma = f"{schema}.candles_1m_ma"

    with psycopg.connect(pg_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop schema if exists {schema} cascade")
            cur.execute(f"create schema {schema}")
            cur.execute(
                f"""
                create table {full_candles} (
                    symbol   text        not null,
                    exchange text        not null,
                    bucket   timestamptz not null,
                    open     numeric(20,8) not null,
                    high     numeric(20,8) not null,
                    low      numeric(20,8) not null,
                    close    numeric(20,8) not null,
                    volume   numeric(20,8) not null,
                    primary key (symbol, exchange, bucket)
                )
                """
            )
            cur.execute(
                f"""
                create table {full_ma} (
                    symbol   text        not null,
                    exchange text        not null,
                    bucket   timestamptz not null,
                    close    numeric(20,8) not null,
                    ma_20    numeric(20,8),
                    primary key (symbol, exchange, bucket)
                )
                """
            )

            base = datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc)
            for closes in [(100, 101, 102, 103, 104)]:
                for j, c in enumerate(closes):
                    bucket = base + timedelta(minutes=j)
                    cur.execute(
                        f"insert into {full_candles} "
                        "(symbol, exchange, bucket, open, high, low, close, volume) "
                        "values (%s,%s,%s,%s,%s,%s,%s,%s)",
                        ("BTCUSD", "binance", bucket, c, c + 1, c - 1, c, Decimal("1.0")),
                    )
                    cur.execute(
                        f"insert into {full_ma} "
                        "(symbol, exchange, bucket, close, ma_20) "
                        "values (%s,%s,%s,%s,%s)",
                        ("BTCUSD", "binance", bucket, c, Decimal(c)),
                    )

    yield schema

    with psycopg.connect(pg_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"drop schema if exists {schema} cascade")
