"""
reclaimer.py — recovers work orphaned by a CRASHED worker.

If a worker dies mid-task it never XACKs, so the message sits in the consumer
group's Pending Entries List forever. XAUTOCLAIM transfers any message idle
longer than `visibility_timeout_ms` to this consumer, and we re-process it.

This is the other half of at-least-once: handler *failures* are handled by the
retry/DLQ path; worker *crashes* are handled here. (Both imply handlers should
be idempotent — a task may run more than once.)
"""

from __future__ import annotations

import asyncio


class Reclaimer:
    def __init__(self, worker, interval: float = 5.0) -> None:
        self.worker = worker
        self.redis = worker.redis
        self.s = worker.s
        self.interval = interval

    async def run(self) -> None:
        while True:
            await self._reclaim_once()
            await asyncio.sleep(self.interval)

    async def _reclaim_once(self) -> None:
        cursor = "0-0"
        while True:
            res = await self.redis.xautoclaim(
                self.s.stream, self.s.group, self.worker.name,
                min_idle_time=self.s.visibility_timeout_ms, start_id=cursor,
                count=self.s.batch_size,
            )
            # redis-py returns [next_cursor, [(id, fields), ...], [deleted_ids]]
            cursor, messages = res[0], res[1]
            for msg_id, fields in messages:
                if fields:                       # skip tombstones for deleted entries
                    await self.worker._process(msg_id, fields)
            if cursor == "0-0" or not messages:  # caught up
                break
