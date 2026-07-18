"""Integration test: prove the Bronze upsert is idempotent on the business key.

Uses a private test schema to avoid touching `bronze.prices_raw` directly.
The test creates a fresh schema + table mirroring the canonical contract,
calls `upsert_to_bronze` twice with identical business keys, and asserts
that the second call inserts 0 rows. Finally, a direct insert (without
ON CONFLICT) hits the unique constraint as a structural sanity check.
"""
from __future__ import annotations

from datetime import datetime, timezone

import psycopg
import pytest

from streaming.upsert import upsert_to_bronze

pytestmark = pytest.mark.integration


SCHEMA = "streaming_test"
TABLE = "prices_raw"


def _make_test_table(conn) -> str:
    """Create a schema + table mirroring bronze.prices_raw. Returns the FQN."""
    full = f"{SCHEMA}.{TABLE}"
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(f"drop schema if exists {SCHEMA} cascade")
            cur.execute(f"create schema {SCHEMA}")
            cur.execute(
                f"""
                create table {full} (
                    id            bigint generated always as identity primary key,
                    symbol        text        not null,
                    exchange      text        not null,
                    price         numeric(20,8) not null,
                    volume        numeric(20,8),
                    event_time    timestamptz not null,
                    ingested_at   timestamptz not null default now(),
                    source        text        not null,
                    raw           jsonb,
                    constraint unique_business_key
                      unique (symbol, exchange, event_time),
                    constraint price_positive
                      check (price > 0)
                )
                """
            )
    return full


def _seed_rows():
    """Return a fixed batch of 3 ticks (same event_time across the symbol
    would dedupe — use distinct event_times for the first insert)."""
    return [
        {
            "symbol": "BTCUSD",
            "exchange": "binance",
            "price": 100.0,
            "volume": 1.0,
            "event_time": datetime(2026, 6, 9, 12, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 6, 9, 12, 0, 1, tzinfo=timezone.utc),
            "source": "freecryptoapi",
            "raw": {"hello": "world"},
        },
        {
            "symbol": "ETHUSD",
            "exchange": "kraken",
            "price": 200.0,
            "volume": 2.0,
            "event_time": datetime(2026, 6, 9, 12, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 6, 9, 12, 0, 1, tzinfo=timezone.utc),
            "source": "freecryptoapi",
            "raw": {"hello": "world"},
        },
        {
            "symbol": "SOLUSD",
            "exchange": "coinbase",
            "price": 50.0,
            "volume": 5.0,
            "event_time": datetime(2026, 6, 9, 12, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2026, 6, 9, 12, 0, 1, tzinfo=timezone.utc),
            "source": "freecryptoapi",
            "raw": {"hello": "world"},
        },
    ]


def _count(conn, full_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {full_table}")
        return cur.fetchone()[0]


def test_first_insert_writes_all_rows(postgres_connection):
    full = _make_test_table(postgres_connection)
    rows = _seed_rows()
    result = upsert_to_bronze(postgres_connection, rows, bronze_table=full)
    assert result.inserted == 3
    assert result.skipped == 0
    assert _count(postgres_connection, full) == 3


def test_second_insert_with_same_keys_is_skipped(postgres_connection):
    full = _make_test_table(postgres_connection)
    rows = _seed_rows()
    upsert_to_bronze(postgres_connection, rows, bronze_table=full)
    second = upsert_to_bronze(postgres_connection, rows, bronze_table=full)
    assert second.inserted == 0
    assert second.skipped == 3
    # Table row count is unchanged.
    assert _count(postgres_connection, full) == 3


def test_partial_overlap_inserts_only_new(postgres_connection):
    full = _make_test_table(postgres_connection)
    rows = _seed_rows()
    upsert_to_bronze(postgres_connection, rows, bronze_table=full)

    # Mutate one event_time (a new business key), keep two duplicates.
    new_rows = list(rows)
    new_rows[0] = dict(rows[0])
    new_rows[0]["event_time"] = datetime(2026, 6, 9, 12, 0, 5, tzinfo=timezone.utc)
    new_rows[0]["price"] = 999.0
    result = upsert_to_bronze(postgres_connection, new_rows, bronze_table=full)
    assert result.inserted == 1
    assert result.skipped == 2
    assert _count(postgres_connection, full) == 4


def test_unique_constraint_blocks_direct_duplicate(postgres_connection):
    full = _make_test_table(postgres_connection)
    rows = _seed_rows()
    upsert_to_bronze(postgres_connection, rows, bronze_table=full)
    # A direct insert (no ON CONFLICT) for a duplicate key must raise.
    with pytest.raises(psycopg.errors.UniqueViolation):
        with postgres_connection.cursor() as cur:
            cur.execute(
                f"""
                insert into {full}
                  (symbol, exchange, price, event_time, source)
                values (%s, %s, %s, %s, %s)
                """,
                (
                    rows[0]["symbol"],
                    rows[0]["exchange"],
                    rows[0]["price"],
                    rows[0]["event_time"],
                    rows[0]["source"],
                ),
            )


def test_check_constraint_rejects_non_positive_price(postgres_connection):
    full = _make_test_table(postgres_connection)
    bad = list(_seed_rows())
    bad[0] = dict(bad[0])
    bad[0]["price"] = -1.0
    with pytest.raises(psycopg.errors.CheckViolation):
        upsert_to_bronze(postgres_connection, bad, bronze_table=full)