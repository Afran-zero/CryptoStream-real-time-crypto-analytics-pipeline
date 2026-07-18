# 04 — Docker fundamentals

CryptoStream is 11 services. They all run at once. How?

**Docker Compose**. This page explains what Docker is, what
containers are, and how Compose orchestrates everything.

---

## The problem Docker solves

A program needs an environment:

- A specific OS (maybe Ubuntu 22.04).
- Specific libraries (Python 3.11, libpq-dev, ...).
- Specific configuration (env vars, files on disk).
- Specific permissions (network ports, file paths).

If you write the program on your laptop and try to run it on a
colleague's machine, on a server, on a CI runner, you hit the
classic **"works on my machine"** problem. The OS is different,
the libraries are different, the env vars are missing.

Docker solves this by packaging the program together with **its
entire environment** into a single artifact called an *image*.
You can then run that image anywhere Docker is installed, and
you'll get the same behaviour.

```
Without Docker:        With Docker:
  Program                ┌────────────────┐
  + Python 3.11          │  Program       │
  + libpq-dev            │  + Python 3.11 │
  + env vars             │  + libpq-dev   │
  + config files         │  + env vars    │
  + OS-specific paths    │  + config      │
                         │  + OS bits     │
  All must be installed  └────────────────┘
  on every machine.         One image.
                            Runs anywhere.
```

---

## Images and containers

An *image* is the package. It's a snapshot of a filesystem plus
some metadata (what command to run, what ports to expose, what
env vars to set).

A *container* is a running instance of an image. You can start
many containers from the same image; they're isolated from each
other and from the host.

```
Image: postgres:16
        │
        ├──▶ container A (your laptop)
        ├──▶ container B (CI runner)
        └──▶ container C (production)
```

CryptoStream's compose file starts 11 containers, all from
different images (some shared, some built locally).

---

## Where do images come from?

Two sources:

1. **Docker Hub** (or another registry) — public images like
   `postgres:16`, `apache/kafka:3.8.0`, `apache/spark:3.5.1`.
   You reference them by name; Docker downloads them on first
   use.
2. **Local builds** — you write a `Dockerfile` that says how to
   build the image, then `docker build` it. CryptoStream builds
   images this way for the `ingestion`, `streaming`, `api`, and
   `dashboard` services.

A Dockerfile is short and declarative:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

Each line is a *layer*. Layers are cached, so editing one file and
rebuilding reuses the unchanged layers and is fast.

---

## Volumes — persistent storage

Containers are **ephemeral** by default. If a container dies, all
its filesystem changes are gone.

That's a problem for things like Postgres data: we don't want to
lose every price tick every time the container restarts.

A *volume* is a piece of storage that lives outside the container's
filesystem. You mount it into the container at a path, and writes
to that path are kept even when the container dies.

```yaml
postgres:
  volumes:
    - pg_data:/var/lib/postgresql/data   # data dir lives on the host
```

CryptoStream uses two named volumes:

- `pg_data` — Postgres's data directory (and the Airflow metadata
  DB, which lives in the same Postgres instance).
- `spark_checkpoints` — Spark's checkpoint state for the streaming
  query.

Named volumes are managed by Docker. To see them:

```bash
docker volume ls | grep cryptostream
```

To wipe one (destructive):

```bash
docker volume rm cs_pg_data
```

---

## Networks — services talking to each other

By default, every container has its own network namespace; it can
talk to the internet but not to other containers.

`docker compose` creates a user-defined bridge network and puts
every service on it. On that network, services can reach each
other by **service name**:

```yaml
services:
  postgres:
    ...
  api:
    environment:
      DATABASE_URL: postgresql://cryptostream:cryptostream@postgres:5432/cryptostream
```

The `api` service can talk to Postgres at hostname `postgres` —
because Compose added a DNS entry for it on the bridge network.
You don't have to know the IP address; the name just works.

The `cryptostream` network in our compose file is the user-defined
bridge. The default bridge that Compose creates automatically
would also work, but a named network makes the topology
self-documenting.

---

## Ports — exposing to the host

A container listens on ports internally (e.g. Postgres on 5432).
By default, those ports aren't accessible from your host
machine.

To expose them, you map them:

```yaml
postgres:
  ports:
    - "5432:5432"   # host_port:container_port
```

CryptoStream exposes:

| Service     | Port  | Why |
|-------------|-------|-----|
| postgres    | 5432  | (debug — to run psql from the host) |
| airflow-webserver | 8080 | so you can open the UI |
| api         | 8000  | so you can hit the REST endpoints |
| dashboard   | 5173  | so you can open the React app |

Services that don't need host access (kafka-init, dbt,
airflow-scheduler, ingestion, spark) don't expose ports — they're
called from inside the network only.

---

## Health checks — knowing when "up" means "ready"

A container can be "running" but not actually serving yet (e.g.
Postgres is still starting up). Compose supports `healthcheck` to
gate `depends_on`:

```yaml
postgres:
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U $POSTGRES_USER -d $POSTGRES_DB"]
    interval: 2s
    timeout: 5s
    retries: 10

api:
  depends_on:
    postgres:
      condition: service_healthy   # not "service_started"
```

This is why cold-boot takes 30–60 s. The graph is:

```
postgres ──▶ kafka ──▶ kafka-init
        └─▶ airflow-init ──▶ airflow-webserver
                         └─▶ airflow-scheduler
        └─▶ ingestion
        └─▶ dbt (only on demand)
        └─▶ api ──▶ dashboard
```

Every arrow is "wait for healthy before starting".

---

## The compose file

CryptoStream's `docker-compose.yml` is the source of truth for
**what runs**. It declares:

- **services** — each container and its image/env/volumes/ports.
- **volumes** — named storage (`pg_data`, `spark_checkpoints`).
- **networks** — the `cryptostream` bridge.
- **dependencies** — `depends_on` with healthcheck gating.

A few patterns worth knowing:

### YAML anchors (DRY)

```yaml
x-airflow-common: &airflow-common
  AIRFLOW__CORE__EXECUTOR: ${AIRFLOW__CORE__EXECUTOR}
  POSTGRES_HOST: postgres
  ...

services:
  airflow-webserver:
    environment:
      <<: *airflow-common    # merges the anchor in
```

The `&airflow-common` defines a chunk; `<<: *airflow-common`
splices it in. This keeps all three airflow services in sync.

### `env` interpolation

```yaml
environment:
  POSTGRES_USER: ${POSTGRES_USER}
```

Compose reads `.env` (or the environment) and substitutes the
value. This is how your `.env` file gets plumbed into containers.

### `restart: unless-stopped`

Most services have this. It means: "if the container crashes,
Docker restarts it; if I explicitly stop it, leave it stopped."

### `restart: on-failure`

The `ingestion` service has this. It means: "if the process exits
non-zero, restart it." Combined with its reconnect loop, this
gives self-healing behaviour.

### `restart: "no"`

`kafka-init` and `airflow-init` are one-shots. They run, do their
job, exit. No restart needed.

---

## Common commands

```bash
# Build and start everything
docker compose up -d --build

# Stop everything (keep volumes)
docker compose down

# Stop and wipe ALL volumes (data loss!)
docker compose down -v

# See what's running
docker compose ps

# Tail logs
docker compose logs -f
docker compose logs -f api

# Run a one-shot command in a service
docker compose run --rm dbt dbt build --no-version-check

# Open a shell in a running container
docker compose exec api bash
docker compose exec postgres psql -U cryptostream
```

The `make` targets wrap these:

| Make target | Equivalent |
|-------------|------------|
| `make up` | `docker compose up -d --build` |
| `make down` | `docker compose down` |
| `make nuke` | `docker compose down -v` |
| `make ps` | `docker compose ps` |
| `make logs` | `docker compose logs -f` |
| `make psql -- -c "SELECT 1"` | `docker compose exec postgres psql ...` |

---

## Why this design for CryptoStream?

| Concern | How Docker helps |
|---------|------------------|
| Postgres, Kafka, Spark, Airflow, dbt, Python, Node — all on one machine | Each runs in its own container with the right OS + libs |
| Multiple services can find each other by name | Compose creates a network with DNS |
| Restart Postgres without losing data | Volume `pg_data` |
| Re-deploy only the API when its code changes | `docker compose up -d --build api` |
| Reproducible on any machine | `docker compose up` works the same on Linux, macOS, Windows |

The alternative (install everything natively) is a world of pain:
you'd spend a day getting the right versions of Postgres + Kafka +
Spark + Airflow installed, and a different day for each teammate's
machine.

---

## Try it yourself

```bash
# See all running containers
docker compose ps

# Inspect a container's metadata
docker compose exec api env | head

# See what's on the named volumes
docker volume inspect cs_pg_data
docker volume inspect cs_spark_checkpoints

# See the bridge network
docker network inspect cryptostream_cryptostream
```

The output of that last command shows every container attached to
the network, with their IP addresses.

---

## Vocabulary

| Term | Meaning |
|------|---------|
| Image | A packaged environment + program, immutable |
| Container | A running instance of an image |
| Dockerfile | Instructions to build an image |
| Volume | Persistent storage outside a container |
| Network | A virtual network shared by some containers |
| Bridge network | A network that does NAT; containers on it can reach each other by name |
| Port mapping | Exposing a container's port to the host |
| Healthcheck | A command run periodically to determine readiness |
| Compose | A YAML file that declares multi-container apps |
| Service | One entry in a Compose file = one container spec |

---

## What's next?

- [05_WEBSOCKETS_FUNDAMENTALS.md](05_WEBSOCKETS_FUNDAMENTALS.md) —
  the kind of connection the ingestion service opens to the
  crypto exchange.
- [06_SPARK_FUNDAMENTALS.md](06_SPARK_FUNDAMENTALS.md) — the
  consumer side, running inside its own container.