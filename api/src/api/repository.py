"""Read-only repository for the Gold layer.

All SQL is parameterized and read-only. The repository accepts the
schema name so a test fixture can point it at a temp schema; in
production the schema is `gold`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg


@dataclass
class HealthStatus:
    db_ok: bool
    freshness_seconds: int | None


class GoldRepository:
    """SQL-bound reads against the Gold schema."""

    def __init__(self, conn: psycopg.Connection, gold_schema: str = "gold") -> None:
        self._conn = conn
        self._schema = gold_schema

    @property
    def _candles_table(self) -> str:
        return f"{self._schema}.candles_1m"

    @property
    def _ma_table(self) -> str:
        return f"{self._schema}.candles_1m_ma"

    def health(self) -> HealthStatus:
        """Single round-trip ping + max(bucket) for freshness."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"select max(bucket) as last_bucket from {self._candles_table}"
                )
                row = cur.fetchone()
        except psycopg.Error:
            return HealthStatus(db_ok=False, freshness_seconds=None)

        if row is None or row[0] is None:
            return HealthStatus(db_ok=True, freshness_seconds=None)

        last_bucket: datetime = row[0]
        if last_bucket.tzinfo is None:
            last_bucket = last_bucket.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - last_bucket
        return HealthStatus(db_ok=True, freshness_seconds=int(delta.total_seconds()))

    def latest_prices(self, symbols: list[str]) -> list[dict[str, Any]]:
        """Most recent candle per (symbol, exchange) for the given symbols.

        `DISTINCT ON` is the idiomatic Postgres replacement for
        `row_number() over (partition by ...)` — one pass, no window.
        """
        if not symbols:
            return []
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                select distinct on (symbol, exchange)
                    symbol, exchange, bucket, close
                from {self._candles_table}
                where symbol = any(%s)
                order by symbol, exchange, bucket desc
                """,
                (symbols,),
            )
            cols = ("symbol", "exchange", "bucket", "close")
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def candles(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        """Most recent `limit` candles for `symbol`, ascending by bucket.

        A bounded `LIMIT N` after an index-friendly `ORDER BY bucket DESC`
        is O(N) instead of the O(symbol_rows) scan that
        `row_number() <= limit` would do.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                select symbol, exchange, bucket, open, high, low, close, volume
                from {self._candles_table}
                where symbol = %s
                order by bucket desc, exchange desc
                limit %s
                """,
                (symbol, limit),
            )
            cols = ("symbol", "exchange", "bucket", "open", "high", "low", "close", "volume")
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        rows.reverse()
        return rows

    def moving_average(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        """Most recent `limit` MA points for `symbol`, ascending by bucket."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                select symbol, exchange, bucket, close, ma_20
                from {self._ma_table}
                where symbol = %s
                order by bucket desc, exchange desc
                limit %s
                """,
                (symbol, limit),
            )
            cols = ("symbol", "exchange", "bucket", "close", "ma_20")
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        rows.reverse()
        return rows


def get_repository(conn: psycopg.Connection, gold_schema: str) -> GoldRepository:
    return GoldRepository(conn, gold_schema=gold_schema)