# 05 — WebSockets fundamentals

CryptoStream gets its data from a **WebSocket** connection to a
crypto exchange. This page explains what a WebSocket is, how it
differs from regular HTTP, and why crypto data uses it.

---

## HTTP — request/response

The web runs on **HTTP**. It's a request/response protocol:

```
Client                              Server
  │ ── GET /api/btc-price ────────▶ │
  │                                  │ (think about it...)
  │ ◀──── 200 OK { "price": 67432 } ─│
```

The client asks a question; the server answers. The connection is
usually closed (or kept idle) after the answer.

This is fine for fetching a webpage. But for **streaming real-time
data**, it has problems:

- The client has to keep asking "any updates?" over and over
  (**polling**), which wastes bandwidth.
- Each request has overhead (headers, TLS handshake, etc.).
- There's no concept of "the server has new data, push it to me".

---

## WebSocket — full duplex

A WebSocket is a different protocol. It starts as an HTTP request,
but the server says "let's upgrade this to a WebSocket", and from
then on **both sides can send messages at any time**:

```
Client                              Server
  │ ── HTTP Upgrade request ───────▶ │
  │ ◀──── 101 Switching Protocols ──│
  │                                  │
  │ ◀──── { "price": 67432 } ───────│ (server pushes)
  │ ◀──── { "price": 67434 } ───────│
  │ ──── { "subscribe": "ETH" } ───▶│ (client sends)
  │ ◀──── { "price": 3520 } ────────│
```

After the handshake, the connection stays open. Both sides can
send whenever they want. This is **full duplex**.

For real-time data, this is exactly what we want:

- The server pushes new prices without being asked.
- The client can send a subscription message ("I want ETH now") at
  any time.
- No polling overhead.

---

## Why WebSockets for crypto prices

Crypto exchanges use WebSockets (or similar push protocols) because:

1. **Latency matters.** A 100ms polling delay is a real difference
   for traders.
2. **Volume matters.** 100 symbols × 10 updates/sec = 1000
   messages/sec. WebSockets handle this without 1000 separate
   HTTP requests.
3. **Server efficiency.** The server doesn't have to deal with
   10,000 clients each polling every second; it just broadcasts.

---

## The protocol in practice

CryptoStream's source is FreeCryptoAPI. The exact protocol
depends on which subscription format the user picks:

### `action_symbols` format (default)

You connect to `wss://api.freecryptoapi.com/ws`, send an auth
message with your API key, then send `subscribe` actions:

```json
{ "action": "subscribe", "symbols": ["BTCUSD", "ETHUSD", "SOLUSD"] }
```

The server then pushes price updates:

```json
{
  "symbol": "BTCUSD",
  "price": 67432.51,
  "volume": 0.5,
  "timestamp": 1721303400
}
```

### `type_channels` format

Different exchanges use different conventions. Some use
"channels":

```json
{ "type": "subscribe", "channels": [{ "name": "ticker", "symbols": ["BTCUSD"] }] }
```

The ingestion service has a `FREECRYPTO_SUBSCRIBE_FMT` env var to
pick between these formats.

---

## What the ingestion service actually does

`ingestion/src/ingestion/ws_client.py` is the WebSocket client.
It uses Python's `websockets` library (asyncio-based).

The flow:

```python
async with websockets.connect(url, extra_headers={"X-API-Key": api_key}) as ws:
    await ws.send(json.dumps(subscribe_message))
    async for message in ws:
        handle_message(message)
```

Key points:

- **`async` / `await`** — WebSocket I/O is non-blocking; one
  coroutine can wait for messages while another sends heartbeats
  or handles a control signal.
- **`async for message in ws`** — iterate over messages as they
  arrive.
- **Reconnect loop** — if the connection drops, reconnect with
  exponential backoff (1s, 2s, 4s, ..., capped at 30s) plus
  jitter.

### Why exponential backoff?

If the server is down for a minute and 100 clients all reconnect
at the same instant, the server gets hammered just as it's coming
back up. **Jitter** randomises the retry delay; **backoff** makes
later retries wait longer. Together they spread the load.

```
Reconnect attempt #1: wait 1s ± jitter
Reconnect attempt #2: wait 2s ± jitter
Reconnect attempt #3: wait 4s ± jitter
...
Reconnect attempt #N: wait min(2^N, 30)s ± jitter
```

---

## The library: `websockets`

Python has several WebSocket libraries. CryptoStream uses
`websockets` (the standalone one, not `websocket-client`).

Why this one?

- **Modern asyncio API** — looks like any other asyncio code.
- **Good error handling** — distinguishes between "connection
  dropped", "invalid message", etc.
- **Mature** — widely used in production.

Other options exist (`aiohttp` for HTTP+WS, `python-socketio`,
`websockets` vs `wsproto`), but for raw WebSocket-on-top-of-TLS,
`websockets` is the standard.

---

## The hand-off

The WebSocket client only deals with raw bytes. It doesn't know
what a "CanonicalTick" is. Once it receives a message, it hands
the bytes to the **normalizer**:

```python
async for message in ws:
    try:
        tick = normalizer.parse(message)
    except NormalizationError as e:
        producer.send_dlq(raw=message, reason=str(e))
        continue
    producer.send(tick)
```

The normalizer:

1. Decodes JSON.
2. Maps the exchange's specific field names to the canonical
   schema (e.g. `"price"` → `price`, `"last"` → `price`, depending
   on the format).
3. Validates with Pydantic (decimal `price`, tz-aware `event_time`,
   etc.).
4. Returns a `CanonicalTick` — or raises an exception.

If the normalizer raises, the message goes to the DLQ. If Kafka
publish fails, the outer loop reconnects and re-tries from the
next message.

---

## Visualising the protocol

```
FreeCryptoAPI                  ingestion service
    │                                  │
    │ ◀───── WebSocket handshake ─────▶│
    │                                  │
    │ ◀───── subscribe BTC,ETH,SOL ────│
    │                                  │
    │ ───── { "price": 67432 } ───────▶│
    │        { "symbol": "BTCUSD" }   │
    │                                  │  parse + validate
    │                                  │  → CanonicalTick
    │                                  │  → Kafka producer
    │                                  │
    │ ───── { "price": 3520 } ────────▶│
    │        { "symbol": "ETHUSD" }   │  → same path
    │                                  │
    │ ───── <connection drops> ────────│
    │                                  │  catch exception
    │                                  │  wait 1s + jitter
    │ ◀───── reconnect ───────────────▶│
    │                                  │
    │ ◀───── re-subscribe ─────────────│
    │                                  │
```

---

## What the WS client doesn't do

A clean WebSocket client is just plumbing. It does **not**:

- Parse messages (that's the normalizer).
- Validate schema (that's Pydantic).
- Publish to Kafka (that's the producer).
- Decide retry policy (that's the reconnect loop, but the logic
  is in this file).

Separation of concerns. Each layer does one thing.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| HTTP | Request/response protocol; client asks, server answers |
| WebSocket | Full-duplex protocol; both sides push at any time |
| `ws://` | Unencrypted WebSocket |
| `wss://` | TLS-encrypted WebSocket |
| Handshake | The initial HTTP request that upgrades to WebSocket |
| Frame | A single message in a WebSocket connection |
| Polling | Repeatedly asking "any updates?" — wasteful |
| Push | Server sends updates without being asked |
| Backoff | Increasing the wait time between retries |
| Jitter | Random variation in wait time; avoids thundering herd |

---

## Try it yourself

Inspect the WebSocket traffic once the stack is running:

```bash
# Tail the ingestion logs
make logs SERVICE=ingestion

# You should see log lines like:
# [INFO] websocket connected to wss://api.freecryptoapi.com/ws
# [INFO] subscribed to ['BTCUSD', 'ETHUSD', 'SOLUSD']
# [INFO] sent tick to Kafka: BTCUSD @ 67432.51
```

If `FREECRYPTO_API_KEY=changeme`, you'll see reconnection attempts.
Set a real key in `.env` and restart: `docker compose up -d
--build ingestion`.

---

## What's next?

- [06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md) — what
  happens to the Kafka messages after ingestion puts them there.
- [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — see the full
  WebSocket → Kafka → Spark → Bronze path traced end to end.