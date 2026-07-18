#!/usr/bin/env bash
# Create the prices + DLQ topics (idempotent), then list them.
# Invoked by the `kafka-init` service in docker-compose.yml.
set -euo pipefail

for t in "$KAFKA_TOPIC_PRICES" "$KAFKA_TOPIC_DLQ"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$t" \
    --partitions 3 \
    --replication-factor 1
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list
