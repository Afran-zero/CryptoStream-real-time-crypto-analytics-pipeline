# 09 — FastAPI fundamentals

CryptoStream exposes the Gold tables via a **FastAPI** service.
This page explains what FastAPI is, how an HTTP API works, and how
the dashboard talks to the database through it.

---

## What is an HTTP API?

An **API** (Application Programming Interface) is how one program
talks to another. The most common style on the web is **HTTP
APIs** (sometimes called REST):

```
Client                                Server
  │ ── GET /prices/latest?symbols=BTC ──▶ │
  │                                        │  (look it up)
  │ ◀──── 200 OK                           │
  │        { "prices": [                  │
  │            {"symbol": "BTCUSD",       │
  │             "price": 67432.51}        │
  │        ] }                            │
```

- The client sends an HTTP request with a **method** (GET, POST,
  etc.) and a **path** (`/prices/latest`).
- The server runs some code, optionally queries a database, and
  returns an HTTP **response** with a status code and a JSON body.

This is the same model your browser uses to fetch webpages; an
HTTP API just returns JSON instead of HTML.

---

## What is FastAPI?

**FastAPI** is a Python framework for building HTTP APIs. You
write functions that take a request and return a response, and
FastAPI handles:

- **Routing** — mapping URLs to functions.
- **Validation** — checking that request data matches a schema.
- **Serialization** — converting Python objects to JSON.
- **OpenAPI docs** — auto-generated interactive documentation at
  `/docs`.
- **Async support** — handle many requests efficiently.

The "Fast" in the name refers to performance (it's on par with
Node.js / Go), but the practical appeal is the developer
experience: type hints + Pydantic = fewer bugs, less boilerplate.

---

## The shape of a FastAPI app

```python
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Price(BaseModel):
    symbol: str
    price: float

@app.get("/prices/latest")
def latest_prices(symbols: str = "BTCUSD"):
    return {"prices": [Price(symbol="BTCUSD", price=67432.51)]}
```

That's a complete API. Visiting `/prices/latest?symbols=BTCUSD`
returns the JSON. Visit `/docs` and you get an interactive page
where you can try the endpoint with different inputs.

CryptoStream's actual app is more layered — but the same shape.

---

## Routing

Each URL your API exposes is a **route**. In FastAPI you declare
them with decorators:

```python
@app.get("/health")                      # GET /health
def health(): ...

@app.get("/prices/latest")               # GET /prices/latest
def latest(symbols: str = "BTCUSD"): ...

@app.get("/candles/{symbol}")            # GET /candles/BTCUSD
def candles(symbol: str, interval: str): ...
```

The path `{symbol}` is a **path parameter**; FastAPI parses it
and passes it as a function argument. Query parameters (`?interval=1m&limit=60`)
become function arguments too.

CryptoStream's routes:

| Method | Path | Returns |
|--------|------|---------|
| GET | `/health` | DB status + freshness seconds |
| GET | `/prices/latest?symbols=A,B,C` | Latest ticks per symbol |
| GET | `/candles/{symbol}?interval=1m&limit=N` | OHLCV candles |
| GET | `/indicators/{symbol}/ma?limit=N` | MA(20) points |
| GET | `/docs` | Swagger UI |
| GET | `/openapi.json` | OpenAPI spec |

---

## Pydantic models

CryptoStream uses **Pydantic** for two purposes:

1. **Request validation** — what the client sends must match the
   schema, or the API returns 422.
2. **Response serialization** — what the API returns is shaped
   exactly per the schema.

Example — the `LatestPrice` model:

```python
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime

class LatestPrice(BaseModel):
    symbol: str
    exchange: str
    price: Decimal
    event_time: datetime
```

If the API returns a `LatestPrice`, FastAPI converts it to JSON
automatically. `Decimal` becomes a string (to preserve precision);
`datetime` becomes an ISO-8601 timestamp.

Why `Decimal` and not `float`? Because:

```python
>>> import json
>>> json.dumps(67432.51)
'67432.51'
>>> json.dumps(float(67432.51))
'67432.51'

>>> json.dumps(0.1 + 0.2)
'0.30000000000000004'   # <- this!
```

`Decimal` serialises to a string in JSON. The client parses the
string back into a `Decimal` (or your language's equivalent).
Prices stay exact.

---

## Dependency injection

CryptoStream's database access goes through a **repository**:

```python
def get_repo() -> GoldRepository:
    cfg = ApiConfig.from_env()
    pool = build_pool(cfg)
    return GoldRepository(pool)

@app.get("/prices/latest")
def latest(symbols: str, repo: GoldRepository = Depends(get_repo)):
    return repo.latest_prices(symbols.split(","))
```

`Depends(get_repo)` is FastAPI's **dependency injection**: before
calling `latest`, FastAPI calls `get_repo` and passes the result
in. The endpoint function never knows how the repo is built —
that's hidden behind the dependency.

Why bother?

- **Testability.** In tests, you `app.dependency_overrides[get_repo]
  = lambda: fake_repo` to swap in a fake.
- **Clean separation.** Endpoints focus on HTTP concerns; the
  repo focuses on database concerns.
- **Single instance.** The pool is built once per process, shared
  across all requests.

---

## Lifespan and startup/shutdown

CryptoStream's app needs a Postgres **connection pool** that lives
as long as the app. FastAPI's modern way to manage that is the
**lifespan** context manager:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = ApiConfig.from_env()
    app.state.pool = build_pool(cfg)
    install_cors(app)
    yield                            # <-- app runs here
    app.state.pool.close()           # <-- on shutdown

app = FastAPI(lifespan=lifespan)
```

When the app starts: build the pool. While requests are served:
the pool is alive. When the app stops: close the pool cleanly.

`app.state` is a generic place to attach things to the app
instance. The endpoints reach it via `request.app.state.pool` or
through a dependency.

---

## CORS

The dashboard runs at `http://localhost:5173`; the API at
`http://localhost:8000`. By default, browsers block JavaScript
from making cross-origin requests (different host **or** different
port).

The server can opt in via **CORS** (Cross-Origin Resource Sharing)
headers:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

This says: "I trust requests from `http://localhost:5173`." The
browser sees the response header and allows the request.

CryptoStream reads the allowed origins from `CORS_ORIGINS` env
var, so you can deploy the dashboard on a different host without
rebuilding.

---

## The repository pattern

`api/src/api/repository.py` is the only file that knows SQL. The
endpoints don't write SQL; they call methods on `GoldRepository`.

```python
class GoldRepository:
    def health(self) -> HealthStatus:
        ...
    def latest_prices(self, symbols: list[str]) -> list[LatestPrice]:
        ...
    def candles(self, symbol: str, limit: int) -> list[Candle]:
        ...
    def moving_average(self, symbol: str, limit: int) -> list[MAPoint]:
        ...
```

This is the **repository pattern**: separate the *what* (what data
do you want?) from the *how* (how do you query Postgres?).

Why?

- **Endpoints stay short.** They orchestrate, not implement.
- **SQL is in one place.** When you switch from psycopg to
  SQLAlchemy, only this file changes.
- **Testability.** Mock the repo in endpoint tests.

---

## Querying Postgres from Python

`psycopg` is the standard Postgres driver for Python. CryptoStream
uses **psycopg 3** with a **connection pool**:

```python
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
    conninfo="postgresql://...",
    min_size=1,
    max_size=8,
    kwargs={"autocommit": True},
)

with pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, price FROM bronze.prices_raw WHERE symbol = %s", ("BTCUSD",))
        rows = cur.fetchall()
```

A few things to know:

- **Placeholders are `%s`**, not `?` or `:1`. (psycopg 3 also
  supports `%(name)s` for named.)
- **Always parameterise.** Never build SQL with f-strings. The
  pool's `connection()` doesn't fix SQL injection; the parameter
  binding does.
- **`with pool.connection()` is a context manager.** It checks a
  connection out of the pool, yields it, and returns it when the
  block exits — even on exceptions.
- **`autocommit=True`** means each statement is its own
  transaction. The repo only does reads, so this is fine.

---

## Why FastAPI?

| Alternative | Why not |
|-------------|---------|
| Flask | Older; no built-in type-driven validation; smaller community now |
| Django | Heavy for a small API; ORM isn't needed |
| aiohttp | More boilerplate; manual schema |
| Plain HTTP server | You'd write all of routing, validation, docs by hand |

FastAPI hits the sweet spot: type hints as the schema, Pydantic
for validation, automatic OpenAPI, async where you want it, sync
where you don't.

---

## Try it yourself

```bash
# Health
curl -sf localhost:8000/health

# Latest prices
curl -sf "localhost:8000/prices/latest?symbols=BTCUSD,ETHUSD"

# Candles
curl -sf "localhost:8000/candles/BTCUSD?interval=1m&limit=10"

# MA
curl -sf "localhost:8000/indicators/BTCUSD/ma?limit=10"

# Open the interactive docs
open http://localhost:8000/docs

# Look at the OpenAPI spec
curl -sf localhost:8000/openapi.json | python -m json.tool | head -40
```

---

## Vocabulary

| Term | Meaning |
|------|---------|
| HTTP API | A way for programs to talk over the web using HTTP |
| REST | A style of HTTP API using URLs + methods |
| Endpoint | A specific URL the API exposes |
| Method | HTTP verb (GET, POST, PUT, DELETE) |
| Status code | A number indicating success/failure (200, 404, 500) |
| JSON | The data format most APIs speak |
| Path parameter | A variable part of the URL (`/candles/{symbol}`) |
| Query parameter | A key-value after `?` in the URL |
| Request body | Data the client sends (usually for POST/PUT) |
| Response body | Data the server returns |
| Pydantic | Python library for typed data validation |
| Dependency injection | FastAPI pattern for passing shared objects to handlers |
| Lifespan | Code that runs at app startup and shutdown |
| CORS | Cross-Origin Resource Sharing; controls who can call your API |
| Repository pattern | A class that encapsulates data access |
| Connection pool | A set of reusable DB connections |

---

## What's next?

- [10_REACT_FUNDAMENTALS.md](10_REACT_FUNDAMENTALS.md) — the
  dashboard that calls this API.
- [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — full end-to-end
  trace including the API layer.