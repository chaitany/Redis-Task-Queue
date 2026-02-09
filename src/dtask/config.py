"""Configuration for the distributed task scheduler."""

import os


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6000/0")

KEY_PREFIX = "dtask"

TASK_HASH = f"{KEY_PREFIX}:task"
QUEUE_PREFIX = f"{KEY_PREFIX}:queue"
PROCESSING_PREFIX = f"{KEY_PREFIX}:processing"
SCHEDULED_SET = f"{KEY_PREFIX}:scheduled"
DEAD_LETTER_SET = f"{KEY_PREFIX}:dead"
WORKER_REGISTRY = f"{KEY_PREFIX}:workers"
WORKER_HEARTBEAT_PREFIX = f"{KEY_PREFIX}:worker:heartbeat"
STATS_PREFIX = f"{KEY_PREFIX}:stats"

HEARTBEAT_INTERVAL_SEC = 5
HEARTBEAT_TIMEOUT_SEC = 30
REAP_INTERVAL_SEC = 15

DEFAULT_QUEUE = "default"
