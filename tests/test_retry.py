"""Pure unit tests for backoff — no infra needed."""

from __future__ import annotations

from streamq.retry import compute_backoff


def test_backoff_within_full_jitter_bounds():
    for attempt in range(1, 6):
        ceiling = min(60.0, 1.0 * 2 ** (attempt - 1))
        for _ in range(50):
            d = compute_backoff(attempt, base=1.0, cap=60.0)
            assert 0.0 <= d <= ceiling + 1e-9


def test_backoff_respects_cap():
    for _ in range(50):
        assert compute_backoff(20, base=1.0, cap=5.0) <= 5.0
