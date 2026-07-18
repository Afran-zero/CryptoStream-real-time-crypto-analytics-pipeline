# Module 3 — Ingestion (WebSocket → Kafka)

## Purpose

Connect to FreeCryptoAPI's WebSocket, normalise every tick into the
canonical schema (decimal prices, UTC `event_time`, fixed field set),
and publish to Kafka with idempotent producer settings. Bad frames go
to a DLQ topic instead of being silently dropped.

## Files

```
ingestion/
  Dockerfile                          # python:3.11-slim + confluent-kafka + websockets
  pyproject.toml                      # package + deps + pytest config
  src/ingestion/
    __init__.py
    config.py                         # Config dataclass from env
    normalizer.py                     # shape-agnostic → CanonicalTick
    producer.py                       # confluent-kafka wrapper (idempotent, acks=all)
    ws_client.py                      # async websockets loop with backoff
    main.py                           # entrypoint: ties it all together
  tests/
    conftest.py
    test_dlq_routing.py
    test_normalizer.py
    test_reconnect.py
    test_ws_integration.py            # real local WS server, marked `@pytest.mark.integration`
```

## Canonical tick

```python
class CanonicalTick(BaseModel):
    symbol: str
    exchange: str
    price: Decimal
    volume: Decimal | None
    event_time: datetime              # tz-aware, UTC
    source: str
    raw: dict[str, Any]               # original payload
```

## How to run

Inside Compose (default — runs automatically with `make up`):

```bash
make up
make logs SERVICE=ingestion
```

Unit tests:

```bash
make test
```

Integration test (spins up a local WebSocket echo server, runs the
real ingest loop against it):

```bash
make test-integration
```

## How to verify

```bash
# Watch live ticks land in Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices --from-beginning --max-messages 5

# DLQ should be empty unless something is malformed
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices.dlq --from-beginning --max-messages 5

# Bronze row count > 0 once Spark is also running
make psql -- -c "select count(*) from bronze.prices_raw;"
```

## Behaviour

- **Reconnect** with exponential backoff: `RECONNECT_INITIAL_S` → `2x`
  each attempt → cap at `RECONNECT_CAP_S`; jitter is added to avoid
  thundering-herd reconnects.
- **Per-message try/except.** Parse/validation failure → DLQ topic with
  reason. Kafka publish failure → re-raise so the reconnect loop
  restarts from the next message.
- **Subscribe format** is configurable (`action_symbols` /
  `type_channels`) because FreeCryptoAPI's protocol uses one of the
  two depending on which version you subscribed to.

## Env vars consumed

See [ENV_REFERENCE.md — Module 3](ENV_REFERENCE.md#module-3--ingestion).

Required: `FREECRYPTO_WS_URL`, `FREECRYPTO_API_KEY`, `WATCHLIST`,
`KAFKA_BOOTSTRAP`. Optional: `KAFKA_TOPIC_PRICES`, `KAFKA_TOPIC_DLQ`,
`SUBSCRIBE_TIMEOUT_S`, `RECONNECT_INITIAL_S`, `RECONNECT_CAP_S`,
`FREECRYPTO_SUBSCRIBE_FMT`.

## Failure modes

| Symptom                                                | Likely cause                                       |
|--------------------------------------------------------|----------------------------------------------------|
| Ingestion restart loop                                 | `FREECRYPTO_API_KEY=changeme` — get a real key     |
| All messages going to `prices.dlq`                     | `FREECRYPTO_SUBSCRIBE_FMT` mismatch — try switching |
| `KafkaTimeoutError` on publish                         | Kafka not ready; ingestion auto-retries            |
| `Watchlist is empty` error on startup                  | `WATCHLIST` is blank                               |

## Tests

```bash
make test                       # unit tests (no Kafka / no live WS)
make test-integration           # real local WS loop
```

Unit tests cover: the normalizer's various input shapes, DLQ
routing on parse failure, exponential-backoff math.

Integration test spins up a local `websockets` server, runs the real
async loop against it, asserts messages appear on Kafka.