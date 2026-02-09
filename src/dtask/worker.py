"""Worker - consumes tasks from Redis queues with fault tolerance."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
import traceback
import uuid
from typing import Any, cast

from .config import (
    TASK_HASH,
    QUEUE_PREFIX,
    PROCESSING_PREFIX,
    DEAD_LETTER_SET,
    WORKER_REGISTRY,
    WORKER_HEARTBEAT_PREFIX,
    HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC,
    REAP_INTERVAL_SEC,
    SCHEDULED_SET,
)
from .connection import get_redis
from .lua_scripts import (
    PROMOTE_SCHEDULED_SCRIPT,
    CLAIM_TASK_SCRIPT,
    COMPLETE_TASK_SCRIPT,
    FAIL_TASK_SCRIPT,
    REQUEUE_ORPHAN_SCRIPT,
)
from .models import Task, TaskState
from .registry import get_handler

logger = logging.getLogger("dtask.worker")


class Worker:
    def __init__(self, queues: list[str] | None = None, concurrency: int = 1):
        self.worker_id = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self._shutdown = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lua_promote: Any = None
        self._lua_claim: Any = None
        self._lua_complete: Any = None
        self._lua_fail: Any = None
        self._lua_requeue: Any = None

    def _register_lua_scripts(self) -> None:
        r = get_redis()
        self._lua_promote = r.register_script(PROMOTE_SCHEDULED_SCRIPT)
        self._lua_claim = r.register_script(CLAIM_TASK_SCRIPT)
        self._lua_complete = r.register_script(COMPLETE_TASK_SCRIPT)
        self._lua_fail = r.register_script(FAIL_TASK_SCRIPT)
        self._lua_requeue = r.register_script(REQUEUE_ORPHAN_SCRIPT)

    def _log(self, level: int, msg: str, **extra: Any) -> None:
        extra["worker_id"] = self.worker_id
        logger.log(level, msg, extra=extra)

    def start(self) -> None:
        self._log(logging.INFO, "Worker starting", event="worker_start",
                  queues=self.queues, concurrency=self.concurrency)
        r = get_redis()

        self._register_lua_scripts()

        r.sadd(WORKER_REGISTRY, self.worker_id)
        self._update_heartbeat()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        heartbeat_t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_t.start()

        scheduler_t = threading.Thread(target=self._scheduler_loop, daemon=True)
        scheduler_t.start()

        reaper_t = threading.Thread(target=self._reaper_loop, daemon=True)
        reaper_t.start()

        for i in range(self.concurrency):
            t = threading.Thread(target=self._work_loop, name=f"worker-thread-{i}", daemon=True)
            t.start()
            self._threads.append(t)

        self._log(logging.INFO, "Worker ready", event="worker_ready")

        try:
            while not self._shutdown.is_set():
                self._shutdown.wait(timeout=1)
        except KeyboardInterrupt:
            pass
        finally:
            self._graceful_shutdown()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self._log(logging.INFO, f"Received signal {signum}, initiating shutdown",
                  event="worker_signal")
        self._shutdown.set()

    def _graceful_shutdown(self) -> None:
        self._log(logging.INFO, "Worker shutting down", event="worker_shutdown")
        self._shutdown.set()

        for t in self._threads:
            t.join(timeout=10)

        r = get_redis()
        r.srem(WORKER_REGISTRY, self.worker_id)
        r.delete(f"{WORKER_HEARTBEAT_PREFIX}:{self.worker_id}")

        processing_key = f"{PROCESSING_PREFIX}:{self.worker_id}"
        orphaned = cast(list[str], r.lrange(processing_key, 0, -1))
        if orphaned:
            self._log(logging.WARNING, f"Re-queuing {len(orphaned)} orphaned tasks",
                      event="worker_requeue_orphans", orphan_count=len(orphaned))
            for task_id in orphaned:
                self._requeue_orphan(task_id)
            r.delete(processing_key)

        self._log(logging.INFO, "Worker stopped", event="worker_stopped")

    def _update_heartbeat(self) -> None:
        r = get_redis()
        r.setex(
            f"{WORKER_HEARTBEAT_PREFIX}:{self.worker_id}",
            HEARTBEAT_TIMEOUT_SEC,
            time.time(),
        )

    def _heartbeat_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                self._update_heartbeat()
            except Exception:
                self._log(logging.ERROR, "Heartbeat failed", event="heartbeat_error")
                logger.debug("Heartbeat exception details", exc_info=True)
            self._shutdown.wait(timeout=HEARTBEAT_INTERVAL_SEC)

    def _scheduler_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                count = self._lua_promote(
                    keys=[SCHEDULED_SET, TASK_HASH],
                    args=[time.time(), QUEUE_PREFIX],
                )
                if count and int(count) > 0:
                    self._log(logging.INFO, f"Promoted {count} scheduled tasks",
                              event="scheduler_promote", promoted_count=int(count))
            except Exception:
                self._log(logging.ERROR, "Scheduler loop error", event="scheduler_error")
                logger.debug("Scheduler exception details", exc_info=True)
            self._shutdown.wait(timeout=2)

    def _reaper_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                self._reap_dead_workers()
            except Exception:
                self._log(logging.ERROR, "Reaper loop error", event="reaper_error")
                logger.debug("Reaper exception details", exc_info=True)
            self._shutdown.wait(timeout=REAP_INTERVAL_SEC)

    def _reap_dead_workers(self) -> None:
        r = get_redis()
        all_workers = cast(set[str], r.smembers(WORKER_REGISTRY))
        for wid in all_workers:
            if wid == self.worker_id:
                continue
            heartbeat_key = f"{WORKER_HEARTBEAT_PREFIX}:{wid}"
            if not r.exists(heartbeat_key):
                processing_key = f"{PROCESSING_PREFIX}:{wid}"
                orphaned = cast(list[str], r.lrange(processing_key, 0, -1))
                self._log(logging.WARNING, f"Reaping dead worker {wid}",
                          event="reaper_dead_worker", dead_worker_id=wid,
                          orphan_count=len(orphaned))
                for task_id in orphaned:
                    self._requeue_orphan(task_id)
                r.delete(processing_key)
                r.srem(WORKER_REGISTRY, wid)

    def _requeue_orphan(self, task_id: str) -> None:
        try:
            result = self._lua_requeue(
                keys=[TASK_HASH],
                args=[QUEUE_PREFIX, task_id, time.time()],
            )
            if result and int(result) > 0:
                self._log(logging.INFO, f"Re-queued orphaned task {task_id}",
                          event="task_requeued", task_id=task_id)
        except Exception:
            self._log(logging.ERROR, f"Failed to requeue orphan task {task_id}",
                      event="task_requeue_error", task_id=task_id)
            logger.debug("Requeue exception details", exc_info=True)

    def _work_loop(self) -> None:
        r = get_redis()
        queue_keys = [f"{QUEUE_PREFIX}:{q}" for q in self.queues]
        processing_key = f"{PROCESSING_PREFIX}:{self.worker_id}"

        while not self._shutdown.is_set():
            try:
                task_id: str | None = None
                for qk in queue_keys:
                    task_id = cast(str | None, r.rpop(qk))
                    if task_id:
                        break

                if task_id is None:
                    self._shutdown.wait(timeout=1)
                    continue

                claimed = self._lua_claim(
                    keys=[TASK_HASH, processing_key],
                    args=[task_id, self.worker_id, time.time()],
                )

                if not claimed or int(claimed) == 0:
                    continue

                self._execute_task(task_id, processing_key)

            except Exception:
                if not self._shutdown.is_set():
                    self._log(logging.ERROR, "Work loop error", event="work_loop_error")
                    logger.debug("Work loop exception details", exc_info=True)
                    time.sleep(1)

    def _execute_task(self, task_id: str, processing_key: str) -> None:
        r = get_redis()
        raw = cast(str | None, r.hget(TASK_HASH, task_id))
        if raw is None:
            r.lrem(processing_key, 1, task_id)
            return

        task = Task.from_json(raw)

        handler = get_handler(task.task_type)
        if handler is None:
            self._log(logging.ERROR, f"No handler for task_type={task.task_type}",
                      event="task_no_handler", task_id=task.task_id,
                      task_type=task.task_type)
            task.state = TaskState.DEAD
            task.error = f"No handler registered for task_type '{task.task_type}'"
            task.touch()
            r.hset(TASK_HASH, task.task_id, task.to_json())
            r.lrem(processing_key, 1, task_id)
            return

        self._log(logging.INFO, f"Executing task {task.task_id}",
                  event="task_start", task_id=task.task_id,
                  task_type=task.task_type, queue=task.queue,
                  attempt=task.attempts, max_attempts=task.max_retries + 1)

        start_time = time.time()

        try:
            result = handler(task.payload)
            result_json = json.dumps(result) if result is not None else "{}"
            elapsed = round(time.time() - start_time, 4)

            self._lua_complete(
                keys=[TASK_HASH, processing_key],
                args=[task_id, result_json, time.time()],
            )
            self._log(logging.INFO, f"Task {task.task_id} succeeded",
                      event="task_success", task_id=task.task_id,
                      task_type=task.task_type, duration_sec=elapsed,
                      attempt=task.attempts)

        except Exception as e:
            elapsed = round(time.time() - start_time, 4)
            tb = traceback.format_exc()
            error_msg = f"{type(e).__name__}: {e}\n{tb}"

            outcome = self._lua_fail(
                keys=[TASK_HASH, processing_key, SCHEDULED_SET, DEAD_LETTER_SET],
                args=[task_id, error_msg, time.time(), task.max_retries, task.retry_delay_sec, task.attempts],
            )

            if outcome == "retry":
                delay = task.retry_delay_sec * (2 ** (task.attempts - 1))
                self._log(logging.WARNING, f"Task {task.task_id} failed, retrying in {delay}s",
                          event="task_retry", task_id=task.task_id,
                          task_type=task.task_type, duration_sec=elapsed,
                          attempt=task.attempts, max_attempts=task.max_retries + 1,
                          delay_sec=delay, error_type=type(e).__name__,
                          error_msg=str(e), outcome="retry")
            elif outcome == "dead":
                self._log(logging.ERROR, f"Task {task.task_id} dead after {task.attempts} attempts",
                          event="task_dead", task_id=task.task_id,
                          task_type=task.task_type, duration_sec=elapsed,
                          attempt=task.attempts, max_attempts=task.max_retries + 1,
                          error_type=type(e).__name__, error_msg=str(e),
                          outcome="dead")
