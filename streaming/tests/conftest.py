"""Pytest fixtures for the streaming service tests.

`integration` tests require a live Postgres reachable via DATABASE_URL.
They are skipped automatically if it is not set.
"""
from __future__ import annotations

import os

import psycopg
import pytest


def _requires_postgres():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set; skipping integration test")


@pytest.fixture
def postgres_connection():
    _requires_postgres()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        yield conn
