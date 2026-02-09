"""Structured JSON logging for the task scheduler."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

EXTRA_FIELDS = (
    "event", "task_id", "task_type", "worker_id", "queue",
    "attempt", "max_attempts", "duration_sec", "delay_sec",
    "error_type", "error_msg", "dead_worker_id", "orphan_count",
    "concurrency", "queues", "promoted_count", "outcome",
)


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        for field in EXTRA_FIELDS:
            val = getattr(record, field, None)
            if val is not None:
                entry[field] = val

        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


def setup_logging(verbose: bool = False, json_output: bool = True) -> None:
    level = logging.DEBUG if verbose else logging.INFO

    root = logging.getLogger()
    root.setLevel(level)

    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    if json_output:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)-5s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))

    root.addHandler(handler)
