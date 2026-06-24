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

---
*Log: Step 1 done (infra). Next: Step 2 — Redis Streams broker + asyncio worker pool.*
