"""Per-client sliding-window rate limiting."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable

from ..exceptions import RateLimitError


class SlidingWindowRateLimiter:
    """Allow at most *max_requests* per client within a sliding time window.

    A true sliding window (per-client deque of timestamps) rather than fixed
    buckets, so a burst straddling a bucket boundary cannot double the
    effective limit.

    Args:
        max_requests: Requests allowed per window per client.
        window_seconds: Window length (default 60 — i.e. requests/minute).
        clock: Injectable monotonic clock, for deterministic tests.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float = 60.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._max = max_requests
        self._window = window_seconds
        self._clock = clock
        self._events: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def check(self, client_id: str) -> None:
        """Record one request for *client_id*, or reject it.

        Raises:
            RateLimitError: If the client is over its budget; carries
                ``retry_after_seconds``.
        """
        now = self._clock()
        cutoff = now - self._window
        with self._lock:
            window = self._events.setdefault(client_id, deque())
            while window and window[0] <= cutoff:
                window.popleft()
            if len(window) >= self._max:
                retry_after = max(0.0, window[0] + self._window - now)
                raise RateLimitError(retry_after)
            window.append(now)

    def reset(self, client_id: str | None = None) -> None:
        """Forget history for one client, or for all clients."""
        with self._lock:
            if client_id is None:
                self._events.clear()
            else:
                self._events.pop(client_id, None)
