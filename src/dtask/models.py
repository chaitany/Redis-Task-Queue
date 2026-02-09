"""Core data models for the task scheduler."""

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"


@dataclass
class Task:
    task_type: str
    payload: dict
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: TaskState = TaskState.PENDING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    attempts: int = 0
    max_retries: int = 3
    retry_delay_sec: int = 5
    timeout_sec: int = 60
    worker_id: str | None = None
    error: str | None = None
    result: Any = None
    queue: str = "default"

    def to_json(self) -> str:
        d = asdict(self)
        d["state"] = self.state.value
        return json.dumps(d)

    @classmethod
    def from_json(cls, raw: str) -> "Task":
        d = json.loads(raw)
        d["state"] = TaskState(d["state"])
        return cls(**d)

    def touch(self):
        self.updated_at = time.time()
