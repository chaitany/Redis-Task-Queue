"""Task producer - enqueue tasks into Redis."""

from __future__ import annotations

import time
from typing import cast

from .config import TASK_HASH, QUEUE_PREFIX, SCHEDULED_SET
from .connection import get_redis
from .lua_scripts import CANCEL_TASK_SCRIPT
from .models import Task, TaskState


def enqueue(
    task_type: str,
    payload: dict,
    queue: str = "default",
    max_retries: int = 3,
    retry_delay_sec: int = 5,
    timeout_sec: int = 60,
    delay_sec: int = 0,
) -> Task:
    r = get_redis()
    task = Task(
        task_type=task_type,
        payload=payload,
        queue=queue,
        max_retries=max_retries,
        retry_delay_sec=retry_delay_sec,
        timeout_sec=timeout_sec,
    )

    if delay_sec > 0:
        task.state = TaskState.PENDING
        pipe = r.pipeline(transaction=True)
        pipe.hset(TASK_HASH, task.task_id, task.to_json())
        pipe.zadd(SCHEDULED_SET, {task.task_id: time.time() + delay_sec})
        pipe.execute()
    else:
        task.state = TaskState.QUEUED
        pipe = r.pipeline(transaction=True)
        pipe.hset(TASK_HASH, task.task_id, task.to_json())
        pipe.lpush(f"{QUEUE_PREFIX}:{queue}", task.task_id)
        pipe.execute()

    return task


def get_task(task_id: str) -> Task | None:
    r = get_redis()
    raw = cast(str | None, r.hget(TASK_HASH, task_id))
    if raw is None:
        return None
    return Task.from_json(raw)


def list_tasks(state: TaskState | None = None, limit: int = 50) -> list[Task]:
    r = get_redis()
    all_raw = cast(list[str], r.hvals(TASK_HASH))
    tasks: list[Task] = []
    for raw in all_raw:
        t = Task.from_json(raw)
        if state is None or t.state == state:
            tasks.append(t)
    tasks.sort(key=lambda t: t.created_at, reverse=True)
    return tasks[:limit]


def cancel_task(task_id: str) -> bool:
    r = get_redis()
    lua_cancel = r.register_script(CANCEL_TASK_SCRIPT)
    result = lua_cancel(
        keys=[TASK_HASH, SCHEDULED_SET],
        args=[task_id, QUEUE_PREFIX, time.time()],
    )
    return result is not None and int(cast(int, result)) > 0
