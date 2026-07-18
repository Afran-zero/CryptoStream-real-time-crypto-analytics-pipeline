"""Pytest fixtures for the ingestion service."""
from __future__ import annotations

import pytest

from ingestion.producer import FakePublisher


@pytest.fixture
def fake_publisher() -> FakePublisher:
    """A fresh FakePublisher for each test."""
    return FakePublisher()
