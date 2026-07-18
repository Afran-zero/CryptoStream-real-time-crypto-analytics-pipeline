#!/usr/bin/env python3
"""Idempotent SQL migrations runner for CryptoStream.

Reads `DATABASE_URL` from the environment and applies any pending
`db/migrations/*.sql` files in filename order. Each file is applied in its
own transaction; on success the version (filename) is recorded in
`public.schema_migrations`. On any error the transaction rolls back, the
script exits non-zero, and the operator sees the underlying psycopg error.

Database-agnostic: works against the local Compose Postgres
(`postgres:5432`) and against any external Postgres reachable via
`DATABASE_URL`, including Neon. No special SSL handling is needed here —
the URL's `?sslmode=...` parameter is honored automatically by psycopg.

Usage:
    DATABASE_URL=postgresql://user:pass@host:5432/db python run_migrations.py
    # or via the Makefile targets `make migrate` (Compose) / `make migrate-host`
"""
from __future__ import annotations

import os
import pathlib
import sys

import psycopg

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parent / "migrations"


def _applied_versions(conn: psycopg.Connection) -> set[str]:
    """Return the set of version filenames already in the ledger."""
    with conn.cursor() as cur:
        cur.execute("select version from public.schema_migrations")
        return {row[0] for row in cur.fetchall()}


def _discover_migration_files() -> list[pathlib.Path]:
    """Sorted list of *.sql files in the migrations directory."""
    return sorted(p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file())


def _version_from_path(path: pathlib.Path) -> str:
    """The filename (e.g. `0002_bronze_prices_raw.sql`) is the version key."""
    return path.name


def _ensure_ledger(conn: psycopg.Connection) -> None:
    """Create the migrations ledger if it does not already exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists public.schema_migrations (
                version    text primary key,
                applied_at timestamptz not null default now()
            )
            """
        )
    conn.commit()


def _apply_one(conn: psycopg.Connection, path: pathlib.Path) -> None:
    """Apply one migration file in its own transaction. Records the version."""
    version = _version_from_path(path)
    with conn.cursor() as cur:
        # Split on top-level `;` so multi-statement migration files work.
        # psycopg's prepared-statement mode rejects multi-statement SQL; we
        # execute each statement individually instead.
        for stmt in _split_sql_statements(path.read_text(encoding="utf-8")):
            cur.execute(stmt)
        cur.execute(
            "insert into public.schema_migrations (version) values (%s)",
            (version,),
        )
    conn.commit()
    print(f"applied {version}")


def _split_sql_statements(sql: str) -> list[str]:
    """Naive statement splitter for plain DDL.

    Strips `-- ...` comment lines, then splits on `;`. Good enough for our
    migrations which contain no PL/pgSQL dollar-quoted blocks.
    """
    cleaned_lines = [
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    ]
    cleaned = "\n".join(cleaned_lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL is not set; refusing to run.", file=sys.stderr)
        return 2

    files = _discover_migration_files()
    if not files:
        print(f"no migration files found under {MIGRATIONS_DIR}", file=sys.stderr)
        return 2

    with psycopg.connect(dsn) as conn:
        _ensure_ledger(conn)
        applied = _applied_versions(conn)
        pending: list[pathlib.Path] = [
            p for p in files if _version_from_path(p) not in applied
        ]

        if not pending:
            print(f"no pending migrations ({len(files)} already applied)")
            return 0

        for path in pending:
            try:
                _apply_one(conn, path)
            except psycopg.Error:
                conn.rollback()
                raise

        print(f"all migrations applied ({len(pending)} new, {len(applied)} previously)")
        return 0


if __name__ == "__main__":
    sys.exit(main())