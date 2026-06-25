"""
retry.py — exponential backoff with jitter.

Each retry waits exponentially longer (base · 2^(attempt-1)) so a struggling
downstream gets breathing room instead of a thundering herd. The result is
capped, then we apply FULL JITTER (a random value in [0, capped]) so many
workers that failed at the same instant don't all retry at the same instant —
their retries spread out across the window. (This is the AWS-recommended scheme.)
"""

from __future__ import annotations

import random


def compute_backoff(attempt: int, base: float, cap: float) -> float:
    """`attempt` is 1-based (the attempt that just failed)."""
    exp = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0, exp)
