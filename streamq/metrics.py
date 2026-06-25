"""
metrics.py — Prometheus metrics for the queue.

The four the roadmap asks for, plus throughput:
  queue depth            ← QUEUE_DEPTH gauge   (XLEN of the work stream = backlog)
  processing latency     ← TASK_DURATION histogram, labelled by task
  retry rate             ← TASKS_PROCESSED{outcome="failed"} (rate() in PromQL)
  DLQ size               ← DLQ_SIZE gauge

Counters (processed by outcome) are incremented inline in the worker; gauges
(depth, pending, dlq, scheduled) are sampled by a small background loop that
reads Redis, since they're point-in-time facts about the queue, not events.
"""

from __future__ import annotations

import asyncio

from prometheus_client import Counter, Gauge, Histogram, start_http_server

TASKS_PROCESSED = Counter(
    "streamq_tasks_processed_total", "Tasks processed by outcome", ["task", "outcome"]
)  # outcome: succeeded | failed | dead | deferred
TASK_DURATION = Histogram(
    "streamq_task_duration_seconds", "Task handler execution time", ["task"]
)
QUEUE_DEPTH = Gauge("streamq_queue_depth", "Entries on the work stream (backlog)")
PENDING = Gauge("streamq_pending_total", "Delivered-but-unacked (in-flight) messages")
DLQ_SIZE = Gauge("streamq_dlq_size", "Dead-letter queue size")
SCHEDULED_RETRIES = Gauge("streamq_scheduled_retries", "Delayed retries waiting to fire")


def serve_metrics(port: int) -> None:
    start_http_server(port)


async def collect_gauges(broker, interval: float = 5.0) -> None:
    """Periodically sample queue-state gauges from Redis."""
    s = broker.s
    while True:
        try:
            # "queue depth" = the group's LAG (entries not yet delivered to any
            # worker) — the real backlog. XLEN would overcount acked entries.
            groups = await broker.redis.xinfo_groups(s.stream)
            grp = next((g for g in groups if g.get("name") == s.group), None)
            if grp:
                QUEUE_DEPTH.set(grp.get("lag") or 0)
                PENDING.set(grp.get("pending") or 0)
            DLQ_SIZE.set(await broker.redis.xlen(s.dlq_stream))
            SCHEDULED_RETRIES.set(await broker.redis.zcard(s.scheduled_zset))
        except Exception:
            pass  # never let metrics sampling crash the worker
        await asyncio.sleep(interval)
