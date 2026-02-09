"""Redis connection management."""

from __future__ import annotations

import redis

from .config import REDIS_URL

_pool: redis.ConnectionPool | None = None


def get_redis() -> redis.Redis:  # type: ignore[type-arg]
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)
    return redis.Redis(connection_pool=_pool)


def check_connection() -> bool:
    try:
        r = get_redis()
        return bool(r.ping())
    except redis.ConnectionError:
        return False
