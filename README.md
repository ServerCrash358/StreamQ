# streamq

A production-style **distributed task queue** in Python — **no Celery**. Raw
`asyncio` workers, **Redis Streams** as the message broker (consumer groups for
fan-out), and **PostgreSQL** for durable job state. Built for at-least-once
delivery, retry with exponential backoff, a dead-letter queue, per-tenant rate
limiting, and **KEDA**-driven autoscaling on queue depth.

> **Status: COMPLETE (all 6 steps).** A production-style distributed task queue:
> Redis Streams + asyncio workers + Postgres durability, at-least-once delivery,
> retry/backoff/DLQ, crash recovery, per-tenant rate limiting, containerised,
> Kubernetes (StatefulSet) with **KEDA** autoscaling on queue depth, and
> Prometheus metrics.

## Why these pieces (the architecture)

```
producer ──XADD──▶ Redis Stream "streamq:tasks"
                        │  (consumer group "workers" — N workers split the load)
        ┌───────────────┼───────────────┐
        ▼               ▼                ▼
     worker          worker           worker        ← asyncio, concurrency per worker
        │ run handler                                 (XREADGROUP → execute → XACK)
        │ success → XACK + jobs.status=succeeded
        │ failure → retry w/ backoff, or → DLQ "streamq:dlq" after max_retries
        ▼
   PostgreSQL "jobs"  ← durable state: status, attempts, last_error, result
```
- **Redis Streams** gives at-least-once delivery: a message stays *pending* until
  the worker `XACK`s it; if a worker dies, another **reclaims** it (`XAUTOCLAIM`).
- **asyncio** is how each worker runs many tasks concurrently — it is *not* what
  makes the queue distributed; the shared Redis stream is.
- **Postgres** is the system of record so job history survives a Redis flush.

## Build plan

| Step | What | State |
|------|------|-------|
| 1 | Infra: Redis + Postgres, ports, durable `jobs` schema | ✅ done |
| 2 | Core: Redis Streams broker + asyncio worker pool + job state | ✅ done |
| 3 | Reliability: at-least-once (ACK + reclaim), retry+backoff+jitter, DLQ | ✅ done |
| 4 | Per-tenant rate limiting + tests | ✅ done |
| 5 | Docker + Kubernetes (StatefulSet) + KEDA autoscaling on queue depth | ✅ done |
| 6 | Prometheus metrics (queue depth, latency by task, retry rate, DLQ size) | ✅ done |

## Run the infra (Step 1)

```bash
docker compose up -d
docker exec streamq-db-1 psql -U streamq -d streamq -c "\d jobs"
docker exec streamq-redis-1 redis-cli ping
```

| Service  | Host port |
|----------|-----------|
| Postgres | 5436      |
| Redis    | 6381      |

## Layout (so far)

```
streamq/
  config.py        # pydantic-settings (stores, stream topology, retry, rate limit)
  db.py            # asyncpg pool (fail-fast)
  models.py        # JobStatus enum
  registry.py      # task name -> handler (@task decorator; sync or async)
  broker.py        # enqueue: Postgres row + XADD; consumer-group setup
  worker.py        # asyncio consumer: XREADGROUP -> run -> update state -> XACK
  retry.py         # exponential backoff + full jitter
  scheduler.py     # pumps due delayed-retries back onto the stream
  reclaimer.py     # XAUTOCLAIM: recovers messages orphaned by crashed workers
  ratelimit.py     # per-tenant sliding-window limiter (Redis ZSET + Lua)
  metrics.py       # Prometheus counters/gauges/histograms + /metrics server
  tasks_example.py # demo handlers (add, slow_echo, boom)
migrations/001_init.sql   # jobs table + job_status enum + indexes
Dockerfile  docker-compose.yml   # worker image + local stack (db, redis, worker)
k8s/               # namespace, postgres, redis, worker StatefulSet, KEDA ScaledObject
tests/             # backoff (unit) + rate limiter (integration)
```

## Deploy to Kubernetes (Step 5)
```bash
# Build + load the worker image into your cluster (e.g. kind):
docker build -t streamq-worker:latest .
kind load docker-image streamq-worker:latest --name <cluster>
# Install KEDA, then apply:
helm install keda kedacore/keda -n keda --create-namespace
kubectl apply -k k8s/
```
KEDA watches the consumer-group **lag** and scales the worker StatefulSet
1→10 as the backlog grows/drains — autoscaling on queue depth, not CPU.

## Metrics (Step 6)
Each worker serves Prometheus at `:9100/metrics`:
`streamq_queue_depth` (lag) · `streamq_pending_total` · `streamq_dlq_size` ·
`streamq_scheduled_retries` · `streamq_task_duration_seconds` (by task) ·
`streamq_tasks_processed_total{outcome=succeeded|failed|dead|deferred}`.

## Run a worker (Step 2)
```bash
docker compose up -d                       # infra
uv sync --all-extras
uv run python -m streamq.worker            # starts a worker (Ctrl+C to stop)
# enqueue from a Python shell / script:  await Broker.create() then broker.enqueue("add", {"a":1,"b":2})
```
