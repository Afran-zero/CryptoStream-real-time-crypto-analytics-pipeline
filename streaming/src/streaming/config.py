"""Configuration for the streaming job.

Read from env (Compose sets these from `.env`):
- KAFKA_BOOTSTRAP         default kafka:9092
- KAFKA_TOPIC_PRICES      default prices
- DATABASE_URL            required (the medallion DB)
- SPARK_CHECKPOINT_DIR    default /checkpoints/bronze
- SPARK_TRIGGER_INTERVAL_S default "10 seconds"
- BRONZE_TABLE            default bronze.prices_raw
"""
from __future__ import annotations

from dataclasses import dataclass

from cryptostream_common import _optional_str, _require


@dataclass(frozen=True)
class StreamConfig:
    kafka_bootstrap: str
    topic_prices: str
    database_url: str
    checkpoint_dir: str
    trigger_interval_s: str
    bronze_table: str

    @classmethod
    def from_env(cls) -> "StreamConfig":
        return cls(
            kafka_bootstrap=_optional_str("KAFKA_BOOTSTRAP", "kafka:9092"),
            topic_prices=_optional_str("KAFKA_TOPIC_PRICES", "prices"),
            database_url=_require("DATABASE_URL"),
            checkpoint_dir=_optional_str("SPARK_CHECKPOINT_DIR", "/checkpoints/bronze"),
            trigger_interval_s=_optional_str("SPARK_TRIGGER_INTERVAL_S", "10 seconds"),
            bronze_table=_optional_str("BRONZE_TABLE", "bronze.prices_raw"),
        )