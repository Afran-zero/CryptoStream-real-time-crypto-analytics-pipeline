# Module 3 — Ingestion Service → Kafka

## Context & Objective
Build the Python service that connects to the FreeCryptoAPI WebSocket, normalizes
each message to the canonical contract (Module 0 §5), publishes valid ticks to the
`prices` topic, and routes anything malformed to `prices.dlq` instead of crashing.
This is the streaming lane's front door and the producer of the data contract that
Module 4 consumes.

## Prerequisites
- Modules 1 & 2 complete: Kafka topics `prices`/`prices.dlq` exist; Postgres ready.
- Codebase state: `ingestion/` empty except `.gitkeep`.

## Technical Specifications
Package `ingestion` under `ingestion/src/`. Components:
- `config.py` — loads env (`FREECRYPTO_WS_URL`, `FREECRYPTO_API_KEY`, `WATCHLIST`,
  `KAFKA_BOOTSTRAP`, topic names).
- `normalizer.py` — pure function `normalize(raw: dict) -> dict` that maps a source
  message to the canonical shape and raises `ValidationError` on missing/invalid
  fields. Validate with a Pydantic v2 model (`price > 0`, timestamps parseable,
  `symbol`/`exchange` non-empty). Set `ingested_at` to now (UTC),
  `source="freecryptoapi"`.
- `producer.py` — thin wrapper over `confluent_kafka.Producer`; `publish(topic, key,
  value: dict)` serializes JSON, keys by `symbol` for partition locality; flush on
  shutdown.
- `ws_client.py` — async WebSocket loop: subscribe to `WATCHLIST`, read messages,
  and for each: try `normalize` → publish to `prices`; on `ValidationError` or JSON
  error → publish the original payload plus an `error` field to `prices.dlq`.
  Reconnect on disconnect with exponential backoff + jitter (cap ~30s). A single
  bad message must never break the loop.
- `main.py` — entrypoint wiring config → client; structured logging (JSON lines);
  counters for `published`, `dlq`, `reconnects`.

Contracts:
- Valid output = exact Module 0 §5 schema.
- DLQ output = `{ "error": "<reason>", "payload": <original> , "dlq_at": <ts> }`.

## Step-by-Step Implementation Instructions
1. Create `ingestion/pyproject.toml` with deps: `confluent-kafka`, `websockets`,
   `pydantic>=2`, `orjson` (or stdlib json), `python-dotenv`; dev deps: `pytest`.
2. Implement `config.py`, `normalizer.py`, `producer.py`, `ws_client.py`, `main.py`
   as specified. Keep `normalize` pure and import-safe (no network at import time).
3. Create `ingestion/Dockerfile` (python:3.11-slim, install package, `CMD python -m
   ingestion.main`).
4. Add an `ingestion` service to `docker-compose.yml`: build from `ingestion/`, env
   from `.env`, `depends_on` kafka healthy, network `cryptostream`, restart policy
   `on-failure`.
5. Tests in `ingestion/tests/`:
   - `test_normalizer.py` — valid payload → canonical dict; each invalid case
     (missing field, price ≤ 0, bad timestamp) raises `ValidationError`.
   - `test_dlq_routing.py` — feed a batch of mixed good/bad messages through the
     handler with a **fake producer** (captures calls); assert good → `prices`,
     bad → `prices.dlq`, and the loop never raises.
   - Do not require a live socket in unit tests; inject messages directly.

## Verification & Testing Criteria
```bash
# unit tests (no infra needed)
docker compose run --rm ingestion pytest -q

# live smoke test: bring the service up, then observe the topic
docker compose up -d ingestion
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices --max-messages 5 --timeout-ms 20000
# ^ must print 5 JSON messages matching the canonical schema

# DLQ proof: publish a malformed record through the running service's code path.
# (If the live source is unavailable, run the service in a mode that injects one
#  bad synthetic message, or unit test already proves routing — record which.)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic prices.dlq --max-messages 1 --timeout-ms 10000
```
Success = valid ticks visible on `prices` with correct fields, and the DLQ routing
is demonstrated (live or via the passing `test_dlq_routing.py`).

> If `FREECRYPTO_WS_URL`/message shape differs from the assumed format at runtime,
> adjust `normalizer.py` mapping only — the **output** contract must not change.

## Hand-off State
- `ingestion` service running, publishing canonical ticks to `prices`.
- Malformed messages routed to `prices.dlq`; ingestion loop resilient to bad data
  and source disconnects (backoff reconnect).
- The canonical message schema is now a live, verified contract.
Module 4 will consume `prices` and parse exactly this schema into `bronze.prices_raw`.
