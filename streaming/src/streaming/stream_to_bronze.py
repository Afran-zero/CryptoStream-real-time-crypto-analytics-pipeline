"""Spark Structured Streaming job: `prices` topic → bronze.prices_raw.

Run via `make stream` (foreground) or `make stream-bg` (background).
Reads all connection settings from env. Per micro-batch, calls
`streaming.upsert.upsert_to_bronze` against Postgres using a temp
staging table + `INSERT … ON CONFLICT DO NOTHING`.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from typing import Any

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.config import StreamConfig
from streaming.upsert import REQUIRED_FIELDS, derive_jdbc_url, upsert_to_bronze

logger = logging.getLogger("streaming.bronze")


# Module 0 §5 canonical schema. Spark parses JSON into this shape, then
# we drop rows that fail parse (DLQ is upstream's job; Bronze doesn't
# receive malformed frames).
CANONICAL_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("exchange", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("event_time", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("source", StringType()),
    ]
)


def _build_spark(cfg: StreamConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("cryptostream-bronze")
        .config("spark.sql.streaming.checkpointLocation", cfg.checkpoint_dir)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _process_batch(batch_df: Any, batch_id: int, cfg: StreamConfig) -> None:
    """foreachBatch callback — runs in the Spark driver per micro-batch."""
    rows = batch_df.collect()
    if not rows:
        logger.info("bronze.batch.empty", extra={"batch_id": batch_id})
        return

    payload: list[dict[str, Any]] = []
    for r in rows:
        d = r.asDict(recursive=True)
        if any(d.get(f) is None for f in REQUIRED_FIELDS):
            continue
        # Envelope the raw Kafka frame with its source coordinates so a
        # replay can be traced back to the originating offset/partition.
        raw_envelope = {
            "kafka_value": d.get("_raw_value"),
            "kafka_topic": d.get("topic"),
            "kafka_partition": d.get("partition"),
            "kafka_offset": d.get("offset"),
        }
        payload.append(
            {
                "symbol": d["symbol"],
                "exchange": d["exchange"],
                "price": float(d["price"]),
                "volume": float(d["volume"]) if d.get("volume") is not None else None,
                "event_time": d["event_time"],
                "ingested_at": d["ingested_at"],
                "source": d["source"],
                "raw": raw_envelope,
            }
        )

    if not payload:
        logger.info(
            "bronze.batch.empty_after_filter",
            extra={"batch_id": batch_id, "received": len(rows)},
        )
        return

    jdbc_url = derive_jdbc_url(cfg.database_url)

    t0 = time.monotonic()
    with psycopg.connect(cfg.database_url) as conn:
        result = upsert_to_bronze(
            conn,
            payload,
            bronze_table=cfg.bronze_table,
        )
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "bronze.batch.applied",
        extra={
            "batch_id": batch_id,
            "received": len(rows),
            "inserted": result.inserted,
            "skipped": result.skipped,
            "elapsed_ms": elapsed_ms,
            "jdbc_url": jdbc_url,
        },
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        cfg = StreamConfig.from_env()
    except Exception as exc:  # noqa: BLE001 — top-level CLI guard
        logger.error("config.error", extra={"error": str(exc)})
        return 2

    logger.info(
        "bronze.starting",
        extra={
            "kafka_bootstrap": cfg.kafka_bootstrap,
            "topic": cfg.topic_prices,
            "checkpoint_dir": cfg.checkpoint_dir,
            "trigger_interval_s": cfg.trigger_interval_s,
            "bronze_table": cfg.bronze_table,
        },
    )

    spark = _build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.topic_prices)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS _raw_value", "topic", "partition", "offset")
        .withColumn("parsed", from_json(col("_raw_value"), CANONICAL_SCHEMA))
        .select(
            "topic",
            "partition",
            "offset",
            "_raw_value",
            col("parsed.symbol").alias("symbol"),
            col("parsed.exchange").alias("exchange"),
            col("parsed.price").alias("price"),
            col("parsed.volume").alias("volume"),
            col("parsed.event_time").alias("event_time"),
            col("parsed.ingested_at").alias("ingested_at"),
            col("parsed.source").alias("source"),
        )
        .na.drop(subset=list(REQUIRED_FIELDS))
    )

    query = (
        parsed.writeStream.foreachBatch(
            lambda batch_df, batch_id: _process_batch(batch_df, batch_id, cfg)
        )
        .trigger(processingTime=cfg.trigger_interval_s)
        .option("checkpointLocation", cfg.checkpoint_dir)
        .start()
    )

    def _shutdown(signum: int, _frame: Any) -> None:
        logger.info("bronze.shutdown", extra={"signum": signum})
        try:
            query.stop()
        except Exception:  # noqa: BLE001
            pass

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())