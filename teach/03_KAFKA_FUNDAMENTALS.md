# 03 — Kafka fundamentals

Kafka is the **queue** in the middle of CryptoStream. This page
explains what a queue is, why we need one, what Kafka specifically
does, and how CryptoStream uses it.

---

## What's a "queue", really?

A queue is a line. Stuff goes in one end, comes out the other, in
the same order.

```
producer ──▶ [ msg1, msg2, msg3, msg4 ] ──▶ consumer
              (the queue, in order)
```

A queue solves one problem: **producer and consumer work at
different speeds**. The producer can drop messages into the queue
as fast as it wants; the consumer pulls them out whenever it's
ready.

A more advanced queue (like Kafka) also lets you:

- **Replay** old messages (read the same message twice).
- **Have multiple consumers** of the same stream, each seeing every
  message.
- **Scale horizontally** by splitting the queue into pieces
  (partitions).

---

## Why we need a queue

Imagine we connected the ingestion service directly to the
database.

```
WebSocket → ingestion → Postgres
```

What could go wrong?

| Scenario | Without queue | With Kafka |
|----------|---------------|------------|
| Postgres is briefly down | Ticks are lost | Ticks sit in Kafka until Postgres recovers |
| We want to add another consumer (e.g. a fraud detector) | Have to wire it into ingestion | It just subscribes to the topic |
| We want to replay yesterday's data | Ask the source (they probably won't resend) | Just re-read from yesterday's offset |
| Source sends 10× faster than we can write | Buffer overflow / data loss | Kafka buffers; we drain at our pace |

In short: Kafka makes the pipeline **elastic** and **resilient**.

---

## What Kafka actually is

Kafka is a program that runs on a server (or many servers). It:

1. **Stores messages on disk** (durable).
2. **Keeps them in order** within a *partition*.
3. **Tracks which message each consumer has read** via an *offset*
   (a numeric position).
4. **Doesn't delete a message just because someone read it** — you
   configure a retention policy (e.g. "keep for 7 days").

So Kafka is more like a **distributed commit log** than a transient
queue. Think of it as a shared filesystem where each file is a
topic.

```
Topic: prices
┌──────────┬──────────┬──────────┬──────────┬──────────┬───
│ msg @1   │ msg @2   │ msg @3   │ msg @4   │ msg @5   │ ...
└──────────┴──────────┴──────────┴──────────┴──────────┴───
                                              ▲
                                              │
                            consumer's offset (read up to here)
```

Once a message is in Kafka, anyone with the right permissions can
read it from any offset, at any time, until retention expires.

---

## Topics, partitions, offsets

### Topic

A *topic* is a named stream. CryptoStream has two:

- `prices` — the canonical, validated tick stream.
- `prices.dlq` — the dead-letter queue for malformed messages.

Think of a topic as a table in a database. Both have names, both
hold data, both have a schema (or at least a shape).

### Partition

Each topic is split into **partitions**. A partition is an
ordered, append-only sequence of messages.

```
Topic: prices
├── partition 0: [msg 0, msg 1, msg 2, msg 3, ...]
├── partition 1: [msg 0, msg 1, msg 2, msg 3, ...]
└── partition 2: [msg 0, msg 1, msg 2, msg 3, ...]
```

Why split? **Parallelism**. If you have 3 partitions, you can have
3 consumers reading in parallel. Each consumer "owns" some
partitions and processes them independently.

The trade-off: messages across partitions aren't ordered. So
"the 100th message of partition 0" might arrive before "the 5th
message of partition 1". Within a partition, order is guaranteed.

For CryptoStream's `prices` topic we use **1 partition** because
we have a single Spark consumer. We don't need parallelism yet.
If we ever add a second consumer, we'd bump partitions to 2.

### Offset

Each message in a partition has a numeric *offset* starting from 0.

```
partition 0: [msg offset=0, msg offset=1, msg offset=2, ...]
```

A consumer's *position* is its current offset. When the consumer
reads message 5, its offset becomes 6.

This is what makes Kafka **replayable**: if your consumer crashes,
when it restarts it can resume from offset 6 — or, if you want to
re-process everything, you can reset to offset 0.

---

## Producers and consumers

### Producer

A *producer* sends messages to a topic.

```python
producer.produce(
    topic="prices",
    key=symbol,                  # partition routing hint
    value=json.dumps(tick).encode(),  # the payload
)
```

The `key` decides which partition the message goes to. If you key
by `symbol`, all messages for BTCUSD land in the same partition —
which means they're in order. Without a key, Kafka round-robins
across partitions.

### Consumer

A *consumer* reads messages from one or more partitions.

```python
consumer = KafkaConsumer(
    "prices",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="latest",   # start from the end if no offset stored
    group_id="spark-bronze",
)
for msg in consumer:
    handle(msg)
```

The `group_id` is what lets multiple consumers share the work.
Kafka automatically assigns partitions to consumers in the same
group: if you have 2 consumers and 4 partitions, each gets 2.

---

## Idempotence and exactly-once semantics

Kafka producers can be configured with `enable.idempotence=true`.
This adds:

- A producer ID (PID) negotiated with the broker.
- Sequence numbers on each message.
- Broker-side deduplication.

Without it, a network hiccup during a write could cause the
producer to retry, resulting in duplicates. With it, the broker
recognises the retry and stores the message exactly once.

This is what lets CryptoStream say "every tick is in Kafka exactly
once". Combined with the unique constraint in Postgres Bronze,
the end-to-end effect is "every tick lands in Bronze exactly once".

---

## DLQ (dead-letter queue)

CryptoStream has a second topic, `prices.dlq`. It's for messages
that the ingestion service couldn't parse or validate:

```
WebSocket → ingestion ──▶ Kafka topic `prices`     (good messages)
                  │
                  └────────▶ Kafka topic `prices.dlq` (bad messages)
```

Why a DLQ instead of just dropping bad messages?

- **Audit.** Someone can later look at `prices.dlq` to see how
  many bad messages came in, and decide whether the source is
  broken.
- **Reprocessing.** If you fix your parser, you can re-read the
  DLQ.
- **Visibility.** "How many ticks failed today?" is a question
  with a real answer.

The DLQ payload includes both the original message and the reason
it failed:

```json
{
  "raw": "<the bytes we couldn't parse>",
  "reason": "JSONDecodeError: Expecting value: line 1 column 1",
  "ts": "2026-07-19T14:30:00.123Z"
}
```

---

## KRaft mode (no Zookeeper)

Older Kafka required a separate **Zookeeper** cluster to manage
cluster metadata. Modern Kafka (3.3+) can run in **KRaft mode**,
where the Kafka brokers themselves elect a controller and manage
metadata without Zookeeper.

CryptoStream uses KRaft because:

- One fewer moving part. We only need to run one Kafka container,
  not two.
- Faster cold boot. No Zookeeper quorum to wait for.
- The future of Kafka. The community is moving away from
  Zookeeper.

The docker-compose.yml shows this in the Kafka environment:

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
```

A single process playing both roles — fine for a demo, fine for
single-broker dev.

---

## Kafka in CryptoStream's compose file

```yaml
kafka:
  image: apache/kafka:3.8.0
  environment:
    KAFKA_NODE_ID: "1"
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_CONTROLLER_LISTENERS: "CONTROLLER://0.0.0.0:9093"
    KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
    KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

kafka-init:
  depends_on:
    kafka:
      condition: service_healthy
  entrypoint: ["bash", "/scripts/create_topics.sh"]
```

Notice:

- `AUTO_CREATE_TOPICS_ENABLE=false` — we create topics explicitly
  via `kafka-init`. This prevents accidental topic creation with
  the wrong partition count.
- `depends_on: condition: service_healthy` — ingestion and spark
  wait for Kafka to be actually serving before they try to
  connect.

---

## Try it yourself

Once the stack is running:

```bash
# List topics
make topics

# Confirm broker is responding
make kafka-versions

# Tail the prices topic (last few messages)
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic prices \
  --from-beginning \
  --max-messages 5

# Tail the DLQ (should usually be empty)
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic prices.dlq \
  --from-beginning \
  --max-messages 5

# See consumer-group offsets (how far Spark has read)
docker compose exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --all-groups --describe
```

The output of that last command shows:

- `GROUP` — the consumer group (e.g. `spark-bronze`)
- `TOPIC` — the topic
- `LAG` — how far behind the consumer is. Zero is good; positive
  means Spark is processing.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| Topic | A named stream of messages |
| Partition | A sub-stream; ordered within, parallel across |
| Offset | A numeric position in a partition |
| Producer | Code that writes to a topic |
| Consumer | Code that reads from a topic |
| Consumer group | A set of consumers sharing work via partition assignment |
| Idempotent producer | A producer that won't duplicate on retry |
| Retention | How long Kafka keeps messages |
| KRaft | Kafka's modern mode without Zookeeper |
| DLQ | A topic for messages the main pipeline couldn't handle |

---

## What's next?

- [04_DOCKER_FUNDAMENTALS.md](04_DOCKER_FUNDAMENTALS.md) — how all
  11 services (including Kafka) run side by side on your machine.
- [06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md) — the
  consumer side of this queue.