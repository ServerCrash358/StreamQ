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

---
*Log: Steps 1–2 done. Next: Step 3 — at-least-once reclaim, retry+backoff+jitter, DLQ.*
