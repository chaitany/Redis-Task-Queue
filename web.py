"""Web demo for dtask: HTTP API + live dashboard + an embedded worker.

The core scheduler (src/dtask) is CLI-driven. This module adds a thin web layer
so the system can be exercised from a browser on a single hosted instance:

- An embedded Worker runs in background threads inside the web process
  (the same heartbeat / scheduler / reaper loops the CLI worker uses).
- Task submission goes through the normal producer (`enqueue`).
- Guardrails keep a public demo cheap and safe: an allowlist of task types,
  clamped payloads, a per-IP submit limit, and a cap on stored tasks.

Run locally:
    REDIS_URL=redis://localhost:6379/0 uvicorn web:app --reload
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

sys.path.insert(0, str(Path(__file__).parent / "src"))

from fastapi import FastAPI, HTTPException, Request  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from dtask import tasks as _tasks  # noqa: E402,F401  (registers handlers)
from dtask.config import (  # noqa: E402
    DEAD_LETTER_SET,
    HEARTBEAT_INTERVAL_SEC,
    TASK_HASH,
    WORKER_HEARTBEAT_PREFIX,
    WORKER_REGISTRY,
)
from dtask.connection import check_connection, get_redis  # noqa: E402
from dtask.logging import setup_logging  # noqa: E402
from dtask.models import Task, TaskState  # noqa: E402
from dtask.producer import cancel_task, enqueue, get_task, list_tasks  # noqa: E402
from dtask.registry import list_registered  # noqa: E402
from dtask.worker import Worker  # noqa: E402

logger = logging.getLogger("dtask.web")

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "3"))
MAX_STORED_TASKS = int(os.environ.get("MAX_STORED_TASKS", "300"))
SUBMITS_PER_MINUTE = int(os.environ.get("SUBMITS_PER_MINUTE", "30"))
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

# Only these task types can be submitted from the public web UI.
ALLOWED_TYPES = {"echo", "add", "divide", "slow_job", "flaky_job", "send_email", "cpu_work", "always_fail"}


class EmbeddedWorker(Worker):
    """A Worker that runs in background threads instead of owning the process.

    `Worker.start()` blocks and installs signal handlers, which is only legal on
    the main thread. Here the web server owns the process and its signals, so we
    start the same loops without blocking and shut down when the app stops.
    """

    def start_background(self) -> None:
        r = get_redis()
        self._register_lua_scripts()
        r.sadd(WORKER_REGISTRY, self.worker_id)
        self._update_heartbeat()
        for target in (self._heartbeat_loop, self._scheduler_loop, self._reaper_loop):
            threading.Thread(target=target, daemon=True).start()
        for i in range(self.concurrency):
            t = threading.Thread(target=self._work_loop, name=f"worker-thread-{i}", daemon=True)
            t.start()
            self._threads.append(t)
        self._log(logging.INFO, "Embedded worker ready", event="worker_ready",
                  concurrency=self.concurrency)

    def stop(self) -> None:
        self._graceful_shutdown()


worker: EmbeddedWorker | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker
    setup_logging(verbose=False, json_output=True)
    if not check_connection():
        raise RuntimeError("Cannot connect to Redis. Check REDIS_URL.")
    worker = EmbeddedWorker(queues=["default"], concurrency=WORKER_CONCURRENCY)
    worker.start_background()
    yield
    worker.stop()


app = FastAPI(
    title="dtask: Distributed Task Scheduler",
    description="Redis-backed task queue with Lua-atomic state transitions, "
                "heartbeat-based failure detection and exponential-backoff retries.",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------- guardrails ----------

_submits: dict[str, deque[float]] = defaultdict(deque)
_submits_lock = threading.Lock()


def _client_ip(request: Request) -> str:
    # Render (and most PaaS) terminate TLS at a proxy; the real client is first in XFF.
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str) -> None:
    now = time.time()
    with _submits_lock:
        q = _submits[ip]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= SUBMITS_PER_MINUTE:
            raise HTTPException(429, f"Limit is {SUBMITS_PER_MINUTE} submissions per minute. Try again shortly.")
        q.append(now)


def _clamp_payload(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    p = dict(payload)
    if task_type == "slow_job":
        p["duration"] = max(0.0, min(float(p.get("duration", 3)), 10.0))
    elif task_type == "cpu_work":
        p["iterations"] = max(1, min(int(p.get("iterations", 100_000)), 200_000))
    elif task_type == "flaky_job":
        p["fail_rate"] = max(0.0, min(float(p.get("fail_rate", 0.5)), 1.0))
    return p


def _enforce_storage_cap() -> None:
    """Keep the task hash bounded: drop the oldest finished tasks past the cap."""
    r = get_redis()
    if cast(int, r.hlen(TASK_HASH)) < MAX_STORED_TASKS:
        return
    finished = [t for t in list_tasks(limit=10_000)
                if t.state in (TaskState.SUCCESS, TaskState.DEAD, TaskState.FAILED)]
    finished.sort(key=lambda t: t.created_at)
    to_drop = finished[: max(1, len(finished) // 3)]
    if to_drop:
        pipe = r.pipeline()
        for t in to_drop:
            pipe.hdel(TASK_HASH, t.task_id)
            pipe.srem(DEAD_LETTER_SET, t.task_id)
        pipe.execute()


# ---------- API ----------

class SubmitRequest(BaseModel):
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(3, ge=0, le=5)
    retry_delay_sec: int = Field(2, ge=1, le=10)
    delay_sec: int = Field(0, ge=0, le=60)


def _task_dict(t: Task) -> dict[str, Any]:
    return {
        "task_id": t.task_id,
        "task_type": t.task_type,
        "state": t.state.value,
        "attempts": t.attempts,
        "max_attempts": t.max_retries + 1,
        "payload": t.payload,
        "result": t.result,
        "error": t.error.strip().splitlines()[-1] if t.error else None,
        "worker_id": t.worker_id,
        "created_at": t.created_at,
        "started_at": t.started_at,
        "completed_at": t.completed_at,
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "redis": check_connection()}


@app.post("/api/tasks", status_code=201)
def submit(req: SubmitRequest, request: Request) -> dict[str, Any]:
    if req.task_type not in ALLOWED_TYPES:
        raise HTTPException(400, f"Unknown task type. Allowed: {', '.join(sorted(ALLOWED_TYPES))}")
    _check_rate_limit(_client_ip(request))
    _enforce_storage_cap()
    task = enqueue(
        task_type=req.task_type,
        payload=_clamp_payload(req.task_type, req.payload),
        max_retries=req.max_retries,
        retry_delay_sec=req.retry_delay_sec,
        delay_sec=req.delay_sec,
        timeout_sec=30,
    )
    return _task_dict(task)


@app.get("/api/tasks")
def tasks(state: str | None = None, limit: int = 60) -> list[dict[str, Any]]:
    st = None
    if state:
        try:
            st = TaskState(state)
        except ValueError:
            raise HTTPException(400, f"Invalid state. Valid: {', '.join(s.value for s in TaskState)}")
    return [_task_dict(t) for t in list_tasks(state=st, limit=min(max(limit, 1), 200))]


@app.get("/api/tasks/{task_id}")
def task_detail(task_id: str) -> dict[str, Any]:
    t = get_task(task_id)
    if t is None:
        raise HTTPException(404, "Task not found")
    d = _task_dict(t)
    d["error_full"] = t.error
    return d


@app.post("/api/tasks/{task_id}/cancel")
def cancel(task_id: str) -> dict[str, Any]:
    if not cancel_task(task_id):
        raise HTTPException(409, "Only queued, pending or retrying tasks can be cancelled")
    return {"cancelled": task_id}


@app.get("/api/stats")
def stats() -> dict[str, Any]:
    r = get_redis()
    counts = {s.value: 0 for s in TaskState}
    for raw in cast(list[str], r.hvals(TASK_HASH)):
        counts[Task.from_json(raw).state.value] += 1
    workers = []
    for wid in cast(set[str], r.smembers(WORKER_REGISTRY)):
        ttl = cast(int, r.ttl(f"{WORKER_HEARTBEAT_PREFIX}:{wid}"))
        workers.append({"worker_id": wid, "alive": ttl > 0, "heartbeat_ttl": ttl})
    return {
        "counts": counts,
        "workers": sorted(workers, key=lambda w: not w["alive"]),
        "task_types": list_registered(),
        "heartbeat_interval_sec": HEARTBEAT_INTERVAL_SEC,
    }


@app.post("/api/admin/purge")
def purge(request: Request) -> dict[str, Any]:
    if ADMIN_TOKEN and request.headers.get("x-admin-token") != ADMIN_TOKEN:
        raise HTTPException(403, "Admin token required")
    r = get_redis()
    finished = [t.task_id for t in list_tasks(limit=100_000)
                if t.state in (TaskState.SUCCESS, TaskState.DEAD, TaskState.FAILED)]
    if finished:
        r.hdel(TASK_HASH, *finished)
        r.srem(DEAD_LETTER_SET, *finished)
    return {"purged": len(finished)}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return (Path(__file__).parent / "web" / "index.html").read_text()
