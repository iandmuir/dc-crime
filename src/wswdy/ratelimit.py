"""Simple in-memory IP-based rate limiter (sliding window)."""
import time
from collections import defaultdict, deque


class RateLimiter:
    """Sliding-window rate limiter keyed by arbitrary string (e.g. IP address)."""

    def __init__(self, *, max_requests: int, window_s: int):
        self.max = max_requests
        self.window = window_s
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> bool:
        """Return True and record the request if under limit; False otherwise."""
        now = time.monotonic()
        bucket = self._buckets[key]
        cutoff = now - self.window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= self.max:
            return False
        bucket.append(now)
        return True
