"""Tests for the worker execution logic (requires running Redis)."""

import json
import threading
import time
from typing import Any

from dtask.config import TASK_HASH, QUEUE_PREFIX, PROCESSING_PREFIX, DEAD_LETTER_SET, SCHEDULED_SET
from dtask.connection import get_redis
from dtask.models import Task, TaskState
from dtask.producer import enqueue, get_task, list_tasks
from dtask.registry import register, _handlers
from dtask.worker import Worker


def _run_worker(seconds: float = 3, concurrency: int = 1) -> None:
    w = Worker(queues=["default"], concurrency=concurrency)
    w._register_lua_scripts()

    for i in range(concurrency):
        t = threading.Thread(target=w._work_loop, name=f"test-worker-{i}", daemon=True)
        t.start()
        w._threads.append(t)

    scheduler_t = threading.Thread(target=w._scheduler_loop, daemon=True)
    scheduler_t.start()

    time.sleep(seconds)
    w._shutdown.set()

    for t in w._threads:
        t.join(timeout=5)
    scheduler_t.join(timeout=5)


class TestWorkerExecution:
    def setup_method(self) -> None:
        _handlers.clear()

    def test_successful_task(self) -> None:
        @register("test_echo")
        def handler(payload: dict) -> dict:
            return {"echoed": payload["msg"]}

        task = enqueue("test_echo", {"msg": "hello"})
        _run_worker(3)

        result = get_task(task.task_id)
        assert result is not None
        assert result.state == TaskState.SUCCESS
        assert result.result["echoed"] == "hello"

    def test_failed_task_retries(self) -> None:
        call_count = {"n": 0}

        @register("fail_once")
        def handler(payload: dict) -> dict:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("First attempt fails")
            return {"ok": True}

        task = enqueue("fail_once", {}, max_retries=3, retry_delay_sec=1)
        _run_worker(8)

        result = get_task(task.task_id)
        assert result is not None
        assert result.state == TaskState.SUCCESS
        assert call_count["n"] >= 2

    def test_task_exhausts_retries(self) -> None:
        @register("always_fail")
        def handler(payload: dict) -> dict:
            raise RuntimeError("Always fails")

        task = enqueue("always_fail", {}, max_retries=1, retry_delay_sec=1)
        _run_worker(8)

        result = get_task(task.task_id)
        assert result is not None
        assert result.state == TaskState.DEAD

        r = get_redis()
        assert r.sismember(DEAD_LETTER_SET, task.task_id)

    def test_no_handler_marks_dead(self) -> None:
        task = enqueue("unregistered_type", {})
        _run_worker(3)

        result = get_task(task.task_id)
        assert result is not None
        assert result.state == TaskState.DEAD
        assert "No handler" in (result.error or "")

    def test_delayed_task_promoted(self) -> None:
        @register("delayed_echo")
        def handler(payload: dict) -> dict:
            return {"done": True}

        task = enqueue("delayed_echo", {}, delay_sec=2)
        assert task.state == TaskState.PENDING

        _run_worker(6)

        result = get_task(task.task_id)
        assert result is not None
        assert result.state == TaskState.SUCCESS


class TestWorkerConcurrency:
    def setup_method(self) -> None:
        _handlers.clear()

    def test_multiple_tasks_processed(self) -> None:
        @register("counter")
        def handler(payload: dict) -> dict:
            return {"i": payload["i"]}

        for i in range(5):
            enqueue("counter", {"i": i})

        _run_worker(5, concurrency=2)

        all_tasks = list_tasks()
        success_count = sum(1 for t in all_tasks if t.state == TaskState.SUCCESS)
        assert success_count >= 5
