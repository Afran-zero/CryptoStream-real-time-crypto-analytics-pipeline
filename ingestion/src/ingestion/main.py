"""Entrypoint for the ingestion service.

Wires config → KafkaPublisher → WebSocketIngestor, installs signal
handlers, and emits structured JSON logs with periodic counters.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any

from .config import Config
from .producer import KafkaPublisher
from .ws_client import WebSocketIngestor


# Stdlib `LogRecord` attributes that are part of the record's own schema —
# everything else we treat as `extra={...}` and copy into the JSON payload.
_LOGRECORD_RESERVED = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Minimal structured JSON formatter for `ingestion.*` loggers."""

    def format(self, record: logging.LogRecord) -> str:
        ts = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
        )
        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        # Anything set via `extra={"...": ...}` lands in `__dict__`.
        for key, value in record.__dict__.items():
            if key in _LOGRECORD_RESERVED:
                continue
            try:
                json.dumps(value)
                payload[key] = value
            except TypeError:
                payload[key] = repr(value)
        return json.dumps(payload, ensure_ascii=False)


def _configure_logging() -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    # Tame the confluent-kafka and websockets chatter.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def _counter_logger(ingestor: WebSocketIngestor, stop: asyncio.Event) -> None:
    """Periodically emit published/dlq/reconnects counters."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass
        if stop.is_set():
            break
        logger = logging.getLogger("ingestion.counters")
        logger.info(
            "ingestion.counters",
            extra={
                "received": ingestor.counters.received,
                "published": ingestor.counters.published,
                "dlq": ingestor.counters.dlq,
                "reconnects": ingestor.counters.reconnects,
                "subscribed": ingestor.counters.subscribed,
            },
        )


async def _run(config: Config) -> None:
    publisher = KafkaPublisher(config.kafka_bootstrap)
    ingestor = WebSocketIngestor(config=config, publisher=publisher)
    stop = asyncio.Event()

    def _on_signal(signum: int) -> None:
        logging.getLogger(__name__).info(
            "shutdown.signal", extra={"signum": signum}
        )
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal, int(sig))
        except (NotImplementedError, RuntimeError):
            # Windows / restricted envs: fall back to default handlers.
            signal.signal(sig, lambda *_: _on_signal(int(sig)))

    counter_task = asyncio.create_task(_counter_logger(ingestor, stop))
    run_task = asyncio.create_task(ingestor.run())
    done, _ = await asyncio.wait(
        {counter_task, run_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    stop.set()
    for task in done:
        if task.cancelled():
            continue
        if exc := task.exception():
            logging.getLogger(__name__).error(
                "ingestion.task_failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )
    for task in (counter_task, run_task):
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
    publisher.flush(timeout=10.0)
    logging.getLogger(__name__).info("ingestion.stopped")


def main() -> int:
    _configure_logging()
    log = logging.getLogger("ingestion.main")
    try:
        config = Config.from_env()
    except Exception as exc:  # noqa: BLE001
        log.error("config.error", extra={"error": str(exc)})
        return 2
    log.info(
        "ingestion.starting",
        extra={
            "ws_url": config.ws_url,
            "watchlist": list(config.watchlist),
            "kafka_bootstrap": config.kafka_bootstrap,
            "topic_prices": config.topic_prices,
            "topic_dlq": config.topic_dlq,
        },
    )
    try:
        asyncio.run(_run(config))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())