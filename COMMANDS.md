# streamq — Command Log

## Step 1 — infra + durable schema

```bash
# Start Postgres (:5436) + Redis (:6381). Postgres runs migrations/001_init.sql
# once on first init, creating the job_status enum + jobs table.
docker compose up -d

# Verify
docker exec streamq-db-1 psql -U streamq -d streamq -c "\d jobs"
docker exec streamq-redis-1 redis-cli ping        # -> PONG

# Stop (keeps data volume)
docker compose stop
```

## Step 2 — core queue (broker + worker pool)

```bash
docker compose up -d
uv sync --all-extras

# Run a worker (joins consumer group "workers", long-polls the stream):
uv run python -m streamq.worker

# Enqueue (from another shell / script):
uv run python -c "
import asyncio
from streamq import tasks_example
from streamq.broker import Broker
async def go():
    b = await Broker.create()
    print(await b.enqueue('add', {'a': 2, 'b': 3}))
    await b.close()
asyncio.run(go())
"

# Inspect durable state:
docker exec streamq-db-1 psql -U streamq -d streamq -c \
  "SELECT task_name, status, attempts, result FROM jobs ORDER BY created_at DESC LIMIT 5;"
```
Verified: enqueue → consumer group → asyncio worker runs handler → job marked
'succeeded' with result in Postgres → message XACK'd (0 pending).

## Step 3 — reliability (retry/backoff, DLQ, reclaim)

The worker (`python -m streamq.worker`) now runs three loops concurrently:
consume + **scheduler** (delayed retries) + **reclaimer** (crash recovery).

How it behaves:
- handler fails & attempts < max_retries → job parked in `streamq:scheduled`
  (ZSET, scored by ready-at) with **exponential backoff + full jitter**; the
  scheduler re-enqueues it when due.
- attempts exhausted → job marked `dead` + pushed to `streamq:dlq`.
- worker crashes mid-task → message stays pending → **XAUTOCLAIM** (after
  `visibility_timeout_ms`) hands it to a live worker, which reprocesses it.

Inspect:
```bash
docker exec streamq-db-1 psql -U streamq -d streamq -c \
  "SELECT status, count(*) FROM jobs GROUP BY status;"
docker exec streamq-redis-1 redis-cli XLEN streamq:dlq            # dead-letter size
docker exec streamq-redis-1 redis-cli ZCARD streamq:scheduled     # pending retries
docker exec streamq-redis-1 redis-cli XPENDING streamq:tasks workers   # in-flight/unacked
```
Verified: boom task → retried w/ backoff → attempts=3 → dead + 1 in DLQ; and an
orphaned (pending) message → reclaimed → reprocessed → succeeded.

> Note: because tasks can run more than once (retries + reclaim), **handlers
> must be idempotent.**

## Step 4 — per-tenant rate limiting + tests

A worker checks the tenant's quota BEFORE running a task. Over-quota → the task
is **deferred** (rescheduled, no retry consumed), so one tenant can't starve the rest.

```bash
uv run pytest tests/ -v        # backoff (unit) + limiter allows-then-blocks (integration)

# Inspect a tenant's current window usage:
docker exec streamq-redis-1 redis-cli ZCARD streamq:ratelimit:acme
```
Tunables (env): `RATE_LIMIT_ENABLED`, `RATE_LIMIT_PER_WINDOW`,
`RATE_LIMIT_WINDOW_SECONDS`, `RATE_LIMIT_DEFER_SECONDS`.

Verified: 4 tests pass; with limit=3, enqueuing 6 tasks for one tenant ran exactly
3 and deferred the other 3 (status stayed `queued`, none failed).

---
*Log: Steps 1–4 done. Next: Step 5 — Docker + Kubernetes + KEDA autoscaling on queue depth.*
