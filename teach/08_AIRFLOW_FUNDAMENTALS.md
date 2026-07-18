# 08 — Airflow fundamentals

CryptoStream uses **Airflow** to schedule dbt runs and run a
retention sweep. This page explains what Airflow is, what a DAG
is, and how the three CryptoStream DAGs fit together.

---

## What is Airflow?

**Apache Airflow** is a workflow orchestrator. You define your
work as a graph of tasks; Airflow runs them on a schedule,
retries failures, and gives you a UI to monitor everything.

The pitch: **"cron with a brain."** Plain cron can run a job at
9am. Airflow can run job A, then job B if A succeeded, alert
Slack if C failed, and backfill last Tuesday's run if you ask.

---

## DAGs, tasks, operators

A **DAG** (Directed Acyclic Graph) is a workflow definition:

```
   task_a ──▶ task_b ──▶ task_d
                │
                └──▶ task_c ──▶ task_d
```

- **Directed**: arrows have a direction (A → B means A runs first).
- **Acyclic**: no loops (you can't have A → B → A; that's an
  infinite loop).
- **Graph**: a set of nodes (tasks) and edges (dependencies).

Each node is a **task**. A task is one piece of work — run a
script, call an API, execute SQL. Tasks are usually implemented
with **operators** — pre-built task templates:

| Operator | What it does |
|----------|--------------|
| `BashOperator` | Run a shell command |
| `PythonOperator` | Run a Python function |
| `PostgresOperator` | Run SQL against Postgres |
| `DockerOperator` | Run a command inside a Docker container |
| `KubernetesPodOperator` | Run a command in a k8s pod |
| `@task` (TaskFlow API) | Decorate a Python function as a task |

---

## Schedules

A DAG can have a `schedule_interval`:

| Value | Meaning |
|-------|---------|
| `@daily` | Once a day, at midnight UTC |
| `@hourly` | Once an hour |
| `*/5 * * * *` | Every 5 minutes (cron syntax) |
| `None` | Manual trigger only |

CryptoStream's DAGs:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `transform_dag` | `*/5 * * * *` | Run dbt (rebuild Silver + Gold) |
| `retention_dag` | `@daily` | Delete old Bronze rows |
| `backfill_dag` | `None` (manual) | Run dbt with custom `--vars` |

---

## The execution model

Airflow has two pieces:

1. **Webserver** — the UI you open in your browser. Shows DAGs,
   runs, logs, Gantt charts.
2. **Scheduler** — a long-running process that triggers DAGs when
   their schedule fires, queues their tasks, and sends them to
   executors.

**Executors** determine how tasks run:

| Executor | Where tasks run |
|----------|-----------------|
| `SequentialExecutor` | One at a time, in the scheduler process |
| `LocalExecutor` | In parallel, in the scheduler process (multi-process) |
| `CeleryExecutor` | Distributed across workers (Redis/RabbitMQ broker) |
| `KubernetesExecutor` | Each task in a fresh k8s pod |

CryptoStream uses `LocalExecutor`: tasks run as subprocesses on
the same machine. It's fine for the demo; for high-throughput
production you'd want Celery or k8s.

---

## DAG anatomy — `transform_dag`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="transform_dag",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transform"],
) as dag:
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps --no-version-check",
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --no-version-check",
    )
    deps >> build
```

Walking through:

- `with DAG(...)` — context manager registers the DAG with
  Airflow.
- `dag_id="transform_dag"` — name visible in the UI.
- `schedule_interval="*/5 * * * *"` — every 5 minutes.
- `start_date=datetime(2026, 1, 1)` — when the schedule starts
  counting. Airflow won't run before this.
- `catchup=False` — don't try to run all the missed schedules
  since `start_date`.
- `deps >> build` — `deps` runs first; `build` runs after.

Each `BashOperator` is one task. The `bash_command` is run as a
shell command inside the Airflow container.

---

## DAG anatomy — `retention_dag`

```python
@task
def purge_old_bronze():
    retention_days = int(Variable.get("bronze_retention_days", default_var=7))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            total = 0
            while True:
                cur.execute("""
                    delete from bronze.prices_raw
                    where ctid in (
                        select ctid from bronze.prices_raw
                        where event_time < %s
                        limit 10000
                    )
                """, (cutoff,))
                deleted = cur.rowcount
                conn.commit()
                total += deleted
                if deleted == 0:
                    break
    logging.info("purged %d rows older than %s", total, cutoff)
```

This uses the **TaskFlow API** (`@task` decorator): the function
itself is the task. Airflow infers inputs/outputs from type
hints. No need to wrap in `PythonOperator`.

Walking through the logic:

1. Read the retention window from an Airflow Variable (default
   7 days).
2. Compute the cutoff timestamp.
3. Loop: DELETE up to 10,000 rows older than the cutoff. Repeat
   until 0 rows deleted.
4. Log the total.

### Why batched DELETE?

A single `DELETE FROM bronze.prices_raw WHERE event_time <
'2026-01-01'` would try to delete millions of rows at once.
Postgres has to:

- Hold a lock on the table for the duration.
- Generate a huge WAL (write-ahead log) record.
- Block readers while it works.

By chunking into 10k-row batches, each delete is fast and
readers aren't blocked. The whole job takes a bit longer, but the
system stays responsive.

### Why `ctid`?

`ctid` is a Postgres internal column that uniquely identifies a
row's physical location. Using it in `WHERE ctid IN (SELECT ctid
... LIMIT 10000)` lets us pick "any 10k matching rows" without
locking the whole table.

---

## DAG anatomy — `backfill_dag`

The backfill DAG takes a date range from `dag_run.conf`:

```python
@task
def write_vars_file(run_id: str, **context):
    conf = context["dag_run"].conf or {}
    start = conf["start_date"]
    end = conf["end_date"]
    path = f"/opt/airflow/transforms/.backfill_vars_{run_id}.json"
    with open(path, "w") as f:
        json.dump({"backfill_start": start, "backfill_end": end}, f)
    return path

@task
def cleanup_vars_file(path: str):
    os.unlink(path)

vars_file = write_vars_file(run_id="{{ run_id }}")
build = BashOperator(
    task_id="dbt_build",
    bash_command=f"dbt build --vars @{vars_file}",
    trigger_rule="all_done",
)
cleanup = cleanup_vars_file(vars_file)
vars_file >> build >> cleanup
```

Why `--vars @$file`? dbt accepts `--vars '{...}'` inline or
`--vars @/path/to/file.json` for a file. The file approach avoids
shell escaping nightmares with JSON in a bash command.

The DAG also cleans up the temp file via a `cleanup` task with
`trigger_rule="all_done"` — runs whether `build` succeeded or
failed.

---

## Airflow Variables

Airflow has a key-value store called **Variables**. CryptoStream
uses one:

```
bronze_retention_days = 7
```

It's set by `airflow-init` on first boot, read by `retention_dag`
on every run. To change it:

```bash
docker compose exec airflow-scheduler \
  airflow variables set bronze_retention_days 30
```

Or in the UI: Admin → Variables.

---

## Connections

Airflow also has a **Connections** store. CryptoStream registers
`postgres_default`:

```
postgres_default:
  type: postgres
  host: postgres
  schema: cryptostream
  login: cryptostream
  password: cryptostream
  port: 5432
```

DAGs and operators reference it by ID. The host name (`postgres`)
is the Docker network alias — same pattern as Compose.

`airflow-init` does the registration on first boot:

```bash
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema "${POSTGRES_DB}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-port 5432
```

---

## Why Airflow?

For a single `dbt build` every 5 minutes, plain cron would work:

```
*/5 * * * *  cd /transforms && dbt build
```

So why Airflow?

- **Retries with backoff.** If dbt fails because Postgres was
  briefly unavailable, Airflow retries automatically.
- **UI.** You can see the history of every run, what failed, and
  drill into logs.
- **Dependencies.** dbt_deps must run before dbt_build. Airflow
  enforces this; cron doesn't.
- **Backfill UX.** Triggering yesterday's dbt run with a date
  range is one click in the UI.
- **Extensibility.** Want to add a Slack alert on failure?
  Decorator. Want to add a new retention sweep? New DAG.

---

## The Airflow UI

Open <http://localhost:8080> (`admin` / `admin`). You see:

| View | What it shows |
|------|---------------|
| DAGs list | All registered DAGs with their next run time |
| DAG detail | Tasks, dependencies, recent runs, Gantt chart |
| Task instance | Logs, XCom, attempts |
| Browse → Variables | The Variables store |
| Browse → Connections | The Connections store |
| Browse → Task Instances | Every task that ever ran |

For CryptoStream, the most useful views are:

- DAGs → `transform_dag` → Graph (shows `dbt_deps` → `dbt_build`)
- DAGs → `transform_dag` → Calendar (run history heatmap)
- DAGs → `retention_dag` → Task Instance → Log (when investigating
  a failure)

---

## Try it yourself

```bash
# See registered DAGs
make airflow-list

# Tail the scheduler logs (look for "executor reporting task instance")
make airflow-logs

# Trigger a DAG by hand
make airflow-trigger DAG=transform_dag

# Trigger a backfill
make airflow-trigger DAG=backfill_dag \
  CONF='{"start_date":"2026-06-01","end_date":"2026-06-02"}'

# Open the UI
open http://localhost:8080
```

---

## Vocabulary

| Term | Meaning |
|------|---------|
| Airflow | Workflow orchestrator |
| DAG | Directed Acyclic Graph; one workflow |
| Task | One node in the DAG; one piece of work |
| Operator | A template for a task (Bash, Python, ...) |
| Sensor | A task that waits for an external condition |
| Executor | Determines where tasks run |
| LocalExecutor | Tasks run as subprocesses on the scheduler host |
| Variable | A key-value config item read by tasks at runtime |
| Connection | A named DB/cluster config for operators |
| XCom | "Cross-communication"; tasks can pass small messages |
| `schedule_interval` | When the DAG runs (`*/5 * * * *`, `@daily`, ...) |
| `start_date` | When the schedule starts counting |
| `catchup` | Whether to run all missed schedules on first deploy |

---

## What's next?

- [09_FASTAPI_FUNDAMENTALS.md](09_FASTAPI_FUNDAMENTALS.md) — how
  the dashboard reads Gold tables.
- [12_DESIGN_DECISIONS.md](12_DESIGN_DECISIONS.md) — why Airflow
  specifically, and why these schedules.