"""Redis connection management."""

import redis

from .config import REDIS_URL

_pool: redis.ConnectionPool | None = None


def get_redis() -> redis.Redis:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)
    return redis.Redis(connection_pool=_pool)


def check_connection() -> bool:
    try:
        r = get_redis()
        return r.ping()
    except redis.ConnectionError:
        return False
