"""Tests for the task producer."""

import time

from dtask.models import TaskState
from dtask.producer import enqueue, get_task, list_tasks, cancel_task
from dtask.config import TASK_HASH, QUEUE_PREFIX, SCHEDULED_SET
from dtask.connection import get_redis


class TestEnqueue:
    def test_enqueue_immediate(self):
        task = enqueue("echo", {"message": "hello"})
        assert task.state == TaskState.QUEUED
        assert task.task_type == "echo"
        assert task.payload == {"message": "hello"}

        r = get_redis()
        assert r.hget(TASK_HASH, task.task_id) is not None
        queue_len = r.llen(f"{QUEUE_PREFIX}:default")
        assert queue_len == 1

    def test_enqueue_delayed(self):
        task = enqueue("echo", {"message": "later"}, delay_sec=60)
        assert task.state == TaskState.PENDING

        r = get_redis()
        score = r.zscore(SCHEDULED_SET, task.task_id)
        assert score is not None
        assert score > time.time()

    def test_enqueue_custom_queue(self):
        task = enqueue("echo", {}, queue="high_priority")
        r = get_redis()
        queue_len = r.llen(f"{QUEUE_PREFIX}:high_priority")
        assert queue_len == 1

    def test_enqueue_custom_retries(self):
        task = enqueue("echo", {}, max_retries=10)
        assert task.max_retries == 10


class TestGetTask:
    def test_get_existing(self):
        task = enqueue("echo", {"key": "value"})
        retrieved = get_task(task.task_id)
        assert retrieved is not None
        assert retrieved.task_id == task.task_id
        assert retrieved.payload == {"key": "value"}

    def test_get_nonexistent(self):
        assert get_task("nonexistent_id") is None


class TestListTasks:
    def test_list_all(self):
        enqueue("echo", {"a": 1})
        enqueue("add", {"b": 2})
        tasks = list_tasks()
        assert len(tasks) == 2

    def test_list_by_state(self):
        enqueue("echo", {"a": 1})
        enqueue("echo", {"b": 2}, delay_sec=60)
        queued = list_tasks(state=TaskState.QUEUED)
        pending = list_tasks(state=TaskState.PENDING)
        assert len(queued) == 1
        assert len(pending) == 1

    def test_list_respects_limit(self):
        for i in range(10):
            enqueue("echo", {"i": i})
        tasks = list_tasks(limit=5)
        assert len(tasks) == 5


class TestCancelTask:
    def test_cancel_queued(self):
        task = enqueue("echo", {})
        assert cancel_task(task.task_id) is True
        updated = get_task(task.task_id)
        assert updated is not None
        assert updated.state == TaskState.DEAD

    def test_cancel_pending(self):
        task = enqueue("echo", {}, delay_sec=60)
        assert cancel_task(task.task_id) is True

    def test_cancel_nonexistent(self):
        assert cancel_task("nonexistent") is False
