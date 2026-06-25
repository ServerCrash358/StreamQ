# streamq

A production-style **distributed task queue** in Python — **no Celery**. Raw
`asyncio` workers, **Redis Streams** as the message broker (consumer groups for
fan-out), and **PostgreSQL** for durable job state. Built for at-least-once
delivery, retry with exponential backoff, a dead-letter queue, per-tenant rate
limiting, and **KEDA**-driven autoscaling on queue depth.

> **Status: Step 3 of 6 complete** — distributed queue with full reliability:
> at-least-once delivery, retry w/ exponential backoff + jitter, dead-letter
> queue, and crash recovery via reclaim. Plan below.

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
| 4 | Per-tenant rate limiting + tests | ⬜ |
| 5 | Docker + Kubernetes + KEDA autoscaling on queue depth | ⬜ |
| 6 | Prometheus metrics (queue depth, latency by task, retry rate, DLQ size) | ⬜ |

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
  tasks_example.py # demo handlers (add, slow_echo, boom)
migrations/001_init.sql   # jobs table + job_status enum + indexes
docker-compose.yml        # postgres + redis
```

## Run a worker (Step 2)
```bash
docker compose up -d                       # infra
uv sync --all-extras
uv run python -m streamq.worker            # starts a worker (Ctrl+C to stop)
# enqueue from a Python shell / script:  await Broker.create() then broker.enqueue("add", {"a":1,"b":2})
```
