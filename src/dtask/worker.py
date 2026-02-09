"""Worker - consumes tasks from Redis queues with fault tolerance."""

import json
import logging
import os
import signal
import threading
import time
import traceback
import uuid

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
        self._lua_promote = None
        self._lua_claim = None
        self._lua_complete = None
        self._lua_fail = None
        self._lua_requeue = None

    def _register_lua_scripts(self):
        r = get_redis()
        self._lua_promote = r.register_script(PROMOTE_SCHEDULED_SCRIPT)
        self._lua_claim = r.register_script(CLAIM_TASK_SCRIPT)
        self._lua_complete = r.register_script(COMPLETE_TASK_SCRIPT)
        self._lua_fail = r.register_script(FAIL_TASK_SCRIPT)
        self._lua_requeue = r.register_script(REQUEUE_ORPHAN_SCRIPT)

    def start(self):
        logger.info(f"Worker {self.worker_id} starting, queues={self.queues}, concurrency={self.concurrency}")
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

        logger.info(f"Worker {self.worker_id} ready")

        try:
            while not self._shutdown.is_set():
                self._shutdown.wait(timeout=1)
        except KeyboardInterrupt:
            pass
        finally:
            self._graceful_shutdown()

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown.set()

    def _graceful_shutdown(self):
        logger.info(f"Worker {self.worker_id} shutting down...")
        self._shutdown.set()

        for t in self._threads:
            t.join(timeout=10)

        r = get_redis()
        r.srem(WORKER_REGISTRY, self.worker_id)
        r.delete(f"{WORKER_HEARTBEAT_PREFIX}:{self.worker_id}")

        processing_key = f"{PROCESSING_PREFIX}:{self.worker_id}"
        orphaned = r.lrange(processing_key, 0, -1)
        if orphaned:
            logger.warning(f"Re-queuing {len(orphaned)} orphaned tasks")
            for task_id in orphaned:
                self._requeue_orphan(task_id)
            r.delete(processing_key)

        logger.info(f"Worker {self.worker_id} stopped")

    def _update_heartbeat(self):
        r = get_redis()
        r.setex(
            f"{WORKER_HEARTBEAT_PREFIX}:{self.worker_id}",
            HEARTBEAT_TIMEOUT_SEC,
            time.time(),
        )

    def _heartbeat_loop(self):
        while not self._shutdown.is_set():
            try:
                self._update_heartbeat()
            except Exception:
                logger.exception("Heartbeat failed")
            self._shutdown.wait(timeout=HEARTBEAT_INTERVAL_SEC)

    def _scheduler_loop(self):
        while not self._shutdown.is_set():
            try:
                count = self._lua_promote(
                    keys=[SCHEDULED_SET, TASK_HASH],
                    args=[time.time(), QUEUE_PREFIX],
                )
                if count and int(count) > 0:
                    logger.info(f"Promoted {count} scheduled tasks")
            except Exception:
                logger.exception("Scheduler loop error")
            self._shutdown.wait(timeout=2)

    def _reaper_loop(self):
        while not self._shutdown.is_set():
            try:
                self._reap_dead_workers()
            except Exception:
                logger.exception("Reaper loop error")
            self._shutdown.wait(timeout=REAP_INTERVAL_SEC)

    def _reap_dead_workers(self):
        r = get_redis()
        all_workers = r.smembers(WORKER_REGISTRY)
        for wid in all_workers:
            if wid == self.worker_id:
                continue
            heartbeat_key = f"{WORKER_HEARTBEAT_PREFIX}:{wid}"
            if not r.exists(heartbeat_key):
                logger.warning(f"Reaping dead worker: {wid}")
                processing_key = f"{PROCESSING_PREFIX}:{wid}"
                orphaned = r.lrange(processing_key, 0, -1)
                for task_id in orphaned:
                    self._requeue_orphan(task_id)
                r.delete(processing_key)
                r.srem(WORKER_REGISTRY, wid)

    def _requeue_orphan(self, task_id: str):
        try:
            result = self._lua_requeue(
                keys=[TASK_HASH],
                args=[QUEUE_PREFIX, task_id, time.time()],
            )
            if result and int(result) > 0:
                logger.info(f"Re-queued orphaned task {task_id}")
        except Exception:
            logger.exception(f"Failed to requeue orphan task {task_id}")

    def _work_loop(self):
        r = get_redis()
        queue_keys = [f"{QUEUE_PREFIX}:{q}" for q in self.queues]
        processing_key = f"{PROCESSING_PREFIX}:{self.worker_id}"

        while not self._shutdown.is_set():
            try:
                task_id = None
                for qk in queue_keys:
                    task_id = r.rpop(qk)
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
                    logger.exception("Work loop error")
                    time.sleep(1)

    def _execute_task(self, task_id: str, processing_key: str):
        r = get_redis()
        raw = r.hget(TASK_HASH, task_id)
        if raw is None:
            r.lrem(processing_key, 1, task_id)
            return

        task = Task.from_json(raw)

        handler = get_handler(task.task_type)
        if handler is None:
            logger.error(f"No handler for task_type={task.task_type}")
            task.state = TaskState.DEAD
            task.error = f"No handler registered for task_type '{task.task_type}'"
            task.touch()
            r.hset(TASK_HASH, task.task_id, task.to_json())
            r.lrem(processing_key, 1, task_id)
            return

        logger.info(f"Executing task {task.task_id} (type={task.task_type}, attempt={task.attempts}/{task.max_retries + 1})")

        try:
            result = handler(task.payload)
            result_json = json.dumps(result) if result is not None else "{}"

            self._lua_complete(
                keys=[TASK_HASH, processing_key],
                args=[task_id, result_json, time.time()],
            )
            logger.info(f"Task {task.task_id} succeeded")

        except Exception as e:
            tb = traceback.format_exc()
            error_msg = f"{type(e).__name__}: {e}\n{tb}"
            logger.error(f"Task {task.task_id} failed: {e}")

            outcome = self._lua_fail(
                keys=[TASK_HASH, processing_key, SCHEDULED_SET, DEAD_LETTER_SET],
                args=[task_id, error_msg, time.time(), task.max_retries, task.retry_delay_sec, task.attempts],
            )
            if outcome == "retry":
                delay = task.retry_delay_sec * (2 ** (task.attempts - 1))
                logger.info(f"Task {task.task_id} scheduled for retry in {delay}s")
            elif outcome == "dead":
                logger.warning(f"Task {task.task_id} moved to dead letter queue after {task.attempts} attempts")
