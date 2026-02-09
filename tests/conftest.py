"""Shared test fixtures for dtask tests."""

import pytest
import redis

from dtask.config import REDIS_URL


@pytest.fixture(autouse=True)
def flush_redis():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.flushdb()
    yield
    r.flushdb()


@pytest.fixture
def redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)
