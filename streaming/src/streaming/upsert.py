"""Idempotent upsert helper for the Bronze stream.

Writes a micro-batch into a unique staging table, then runs
`INSERT … SELECT … ON CONFLICT (symbol, exchange, event_time) DO NOTHING`
against the canonical `bronze.prices_raw`. Returns `(inserted, skipped)`
counts derived from PostgreSQL's `RETURNING` semantics.

The signature deliberately avoids depending on PySpark: pure psycopg +
optional pandas, so this module is unit-testable from the host without
Spark in the loop.
"""
from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


BRONZE_COLUMNS: tuple[str, ...] = (
    "symbol",
    "exchange",
    "price",
    "volume",
    "event_time",
    "ingested_at",
    "source",
    "raw",
)

# NOT NULL business key + canonical fields. The same tuple is reused by the
# Spark driver to drop rows that failed parse before they reach Postgres.
REQUIRED_FIELDS: tuple[str, ...] = (
    "symbol",
    "exchange",
    "price",
    "event_time",
    "source",
)


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    skipped: int
    staging_table: str | None  # None when no staging table was created

    @property
    def total(self) -> int:
        return self.inserted + self.skipped


def _stage_name() -> sql.Identifier:
    """Unique per-call staging table name: bronze._prices_raw_stage_<hex>."""
    suffix = secrets.token_hex(6)
    return sql.Identifier(f"_prices_raw_stage_{suffix}")


def _coerce_raw(value: Any) -> Jsonb | None:
    """Pass through dicts / lists / JSON strings as Postgres `jsonb`."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return Jsonb(value)
    if isinstance(value, str):
        try:
            return Jsonb(json.loads(value))
        except json.JSONDecodeError:
            return Jsonb(value)
    raise TypeError(
        f"raw payload must be dict, list, or JSON string; got {type(value).__name__}"
    )


def _normalize_rows(rows: Iterable[Mapping[str, Any]]) -> list[tuple]:
    """Project rows to the canonical column tuple, coercing types."""
    out: list[tuple] = []
    for row in rows:
        if any(row.get(col) is None for col in REQUIRED_FIELDS):
            continue

        volume = row.get("volume")
        out.append(
            (
                str(row["symbol"]),
                str(row["exchange"]),
                row["price"] if isinstance(row["price"], Decimal) else Decimal(str(row["price"])),
                (
                    volume
                    if isinstance(volume, Decimal)
                    else (Decimal(str(volume)) if volume is not None else None)
                ),
                row["event_time"] if isinstance(row["event_time"], datetime) else row["event_time"],
                (
                    row["ingested_at"]
                    if isinstance(row.get("ingested_at"), datetime)
                    else row.get("ingested_at")
                ),
                str(row["source"]),
                _coerce_raw(row.get("raw")),
            )
        )
    return out


def upsert_to_bronze(
    conn: psycopg.Connection,
    rows: Sequence[Mapping[str, Any]],
    *,
    bronze_table: str = "bronze.prices_raw",
    stage_schema: str = "bronze",
) -> UpsertResult:
    """Insert every row into `bronze_table`, idempotently.

    Returns (inserted, skipped) counts from the actual `ON CONFLICT`
    execution. Closes over its own transaction so callers can stay in
    autocommit mode if they like.
    """
    if not rows:
        return UpsertResult(inserted=0, skipped=0, staging_table=None)

    normalized = _normalize_rows(rows)
    if not normalized:
        return UpsertResult(inserted=0, skipped=0, staging_table=None)

    stage_table = _stage_name()
    create_stmt = sql.SQL("create table {}.{} (like {} including all)").format(
        sql.Identifier(stage_schema),
        stage_table,
        sql.Identifier(*bronze_table.split(".")),
    )
    drop_stmt = sql.SQL("drop table if exists {}.{}").format(
        sql.Identifier(stage_schema),
        stage_table,
    )

    insert_into_stage = sql.SQL(
        "insert into {st}.{tbl} ({cols}) values %s"
    ).format(
        st=sql.Identifier(stage_schema),
        tbl=stage_table,
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in BRONZE_COLUMNS),
    )

    target_schema, target_table = bronze_table.split(".", 1)
    insert_into_target = sql.SQL(
        """
        insert into {schema}.{tbl} ({cols})
        select {cols} from {stage_schema}.{stage_tbl}
        on conflict (symbol, exchange, event_time) do nothing
        returning id
        """
    ).format(
        schema=sql.Identifier(target_schema),
        tbl=sql.Identifier(target_table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in BRONZE_COLUMNS),
        stage_schema=sql.Identifier(stage_schema),
        stage_tbl=stage_table,
    )

    with conn.transaction():
        cur = conn.cursor()
        cur.execute(create_stmt)
        psycopg.rows.insert_row_factory = None
        with cur.copy(
            sql.SQL("copy {st}.{tbl} ({cols}) from stdin").format(
                st=sql.Identifier(stage_schema),
                tbl=stage_table,
                cols=sql.SQL(", ").join(sql.Identifier(c) for c in BRONZE_COLUMNS),
            )
        ) as copy:
            for tup in normalized:
                copy.write_row(tup)

        cur.execute(insert_into_target)
        inserted = cur.rowcount
        cur.execute(drop_stmt)

    skipped = len(normalized) - inserted
    return UpsertResult(
        inserted=inserted,
        skipped=skipped,
        staging_table=f"{stage_schema}.{stage_table}",
    )


def derive_jdbc_url(database_url: str) -> str:
    """Convert a libpq-style URL into the JDBC URL Spark's connector wants.

    `postgresql://user:pass@host:5432/db` → `jdbc:postgresql://host:5432/db`.
    """
    p = urlparse(database_url)
    host = p.hostname or "localhost"
    port = p.port or 5432
    path = p.path.lstrip("/") or "postgres"
    return f"jdbc:postgresql://{host}:{port}/{path}"


__all__ = [
    "BRONZE_COLUMNS",
    "REQUIRED_FIELDS",
    "UpsertResult",
    "upsert_to_bronze",
    "derive_jdbc_url",
]