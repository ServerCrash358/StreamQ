"""Shared types."""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"      # failed an attempt; may still retry (Step 3)
    DEAD = "dead"          # gave up after max_retries → dead-letter queue (Step 3)
