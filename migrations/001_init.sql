-- 001_init.sql — durable job state.
--
-- Redis Streams carries the *work* (fast, at-least-once delivery). Postgres is
-- the *system of record*: every job's status, attempt count, last error, and
-- result outlive Redis. If Redis is flushed, you still have the audit trail and
-- can tell what ran, what failed, and what's in the dead-letter queue.

-- Idempotent enum creation (init script only runs on a fresh DB, but be safe).
DO $$ BEGIN
  CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'failed', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name   TEXT        NOT NULL,                 -- which registered handler runs it
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    tenant_id   TEXT        NOT NULL DEFAULT 'default', -- for per-tenant rate limiting
    status      job_status  NOT NULL DEFAULT 'queued',
    attempts    INT         NOT NULL DEFAULT 0,
    max_retries INT         NOT NULL DEFAULT 3,
    last_error  TEXT,
    result      JSONB,
    stream_id   TEXT,                                  -- the Redis stream entry id
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dashboards/queries filter by status (e.g. count 'dead') and by tenant.
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);
