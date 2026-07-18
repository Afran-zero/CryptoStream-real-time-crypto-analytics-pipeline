"""Kafka producer wrapper for the ingestion service.

The `Publisher` Protocol lets tests inject a `FakePublisher` that records
calls without touching Kafka. Both implementations share the same
`publish(topic, key, value)` signature.
"""
from __future__ import annotations

import logging
from typing import Any, Protocol

import orjson
from confluent_kafka import KafkaError, Producer

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    """Anything `WebSocketIngestor` can publish through."""

    def publish(self, topic: str, key: str, value: dict) -> None: ...
    def flush(self, timeout: float = 5.0) -> int: ...


class KafkaPublisher:
    """Production publisher wrapping `confluent_kafka.Producer`."""

    def __init__(self, bootstrap: str, *, flush_timeout: float = 5.0) -> None:
        # `enable.idempotence=true` forces acks=all, retries=∞, max.in.flight=5
        # — it silently overrides the redundant acks/retries we'd otherwise set.
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 5,
                "socket.keepalive.enable": True,
                "client.id": "cryptostream-ingestion",
            }
        )
        self._flush_timeout = flush_timeout

    def _on_delivery(self, err: KafkaError | None, msg: Any) -> None:
        if err is not None:
            logger.error(
                "kafka delivery failed",
                extra={"topic": msg.topic() if msg else "?", "error": str(err)},
            )

    def publish(self, topic: str, key: str, value: dict) -> None:
        # `default=str` lets arbitrary payloads (e.g. unusual raw frames
        # destined for the DLQ) flow through without crashing the loop.
        raw = orjson.dumps(value, default=str)
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=raw,
            on_delivery=self._on_delivery,
        )
        # Non-blocking poll so delivery callbacks fire.
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout if timeout is not None else self._flush_timeout)


class FakePublisher:
    """In-memory publisher for unit tests. Records every call."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict]] = []
        self.published = 0
        self.dlq = 0

    def publish(self, topic: str, key: str, value: dict) -> None:
        self.records.append((topic, key, value))
        if topic.endswith("dlq") or ".dlq" in topic:
            self.dlq += 1
        else:
            self.published += 1

    def flush(self, timeout: float = 5.0) -> int:  # noqa: ARG002 — protocol compat
        return len(self.records)

    # Convenience for tests
    def by_topic(self, topic: str) -> list[tuple[str, dict]]:
        return [(k, v) for t, k, v in self.records if t == topic]


__all__ = ["Publisher", "KafkaPublisher", "FakePublisher"]
