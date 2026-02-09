"""CLI interface for the distributed task scheduler."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import textwrap
import time
from typing import cast

from . import tasks as _tasks_module  # noqa: F401 - registers handlers on import
from .connection import check_connection, get_redis
from .config import TASK_HASH, DEAD_LETTER_SET, WORKER_REGISTRY, WORKER_HEARTBEAT_PREFIX
from .logging import setup_logging
from .models import Task, TaskState
from .producer import enqueue, get_task, list_tasks, cancel_task
from .registry import list_registered
from .worker import Worker


def cmd_submit(args: argparse.Namespace) -> None:
    payload: dict = {}
    if args.payload:
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON payload: {e}")
            sys.exit(1)

    task = enqueue(
        task_type=args.task_type,
        payload=payload,
        queue=args.queue,
        max_retries=args.retries,
        delay_sec=args.delay,
    )
    print(f"Task submitted: {task.task_id}")
    print(f"  type:    {task.task_type}")
    print(f"  state:   {task.state.value}")
    print(f"  queue:   {task.queue}")
    if args.delay > 0:
        print(f"  delay:   {args.delay}s")


def cmd_status(args: argparse.Namespace) -> None:
    task = get_task(args.task_id)
    if task is None:
        print(f"Task {args.task_id} not found")
        sys.exit(1)

    print(f"Task: {task.task_id}")
    print(f"  type:       {task.task_type}")
    print(f"  state:      {task.state.value}")
    print(f"  queue:      {task.queue}")
    print(f"  attempts:   {task.attempts}/{task.max_retries + 1}")
    print(f"  created:    {_fmt_time(task.created_at)}")
    if task.started_at:
        print(f"  started:    {_fmt_time(task.started_at)}")
    if task.completed_at and task.started_at:
        print(f"  completed:  {_fmt_time(task.completed_at)}")
        print(f"  duration:   {task.completed_at - task.started_at:.2f}s")
    if task.worker_id:
        print(f"  worker:     {task.worker_id}")
    if task.result is not None:
        print(f"  result:     {json.dumps(task.result)}")
    if task.error:
        print(f"  error:      {task.error.splitlines()[0]}")


def cmd_list(args: argparse.Namespace) -> None:
    state_filter = None
    if args.state:
        try:
            state_filter = TaskState(args.state)
        except ValueError:
            print(f"Invalid state: {args.state}")
            print(f"Valid states: {', '.join(s.value for s in TaskState)}")
            sys.exit(1)

    found = list_tasks(state=state_filter, limit=args.limit)
    if not found:
        print("No tasks found")
        return

    header = f"{'ID':<14} {'TYPE':<14} {'STATE':<10} {'ATTEMPTS':<10} {'CREATED':<20}"
    print(header)
    print("-" * len(header))
    for t in found:
        print(f"{t.task_id:<14} {t.task_type:<14} {t.state.value:<10} {t.attempts:<10} {_fmt_time(t.created_at):<20}")


def cmd_cancel(args: argparse.Namespace) -> None:
    if cancel_task(args.task_id):
        print(f"Task {args.task_id} cancelled")
    else:
        print(f"Cannot cancel task {args.task_id} (not found or already running)")


def cmd_worker(args: argparse.Namespace) -> None:
    json_logs = getattr(args, "json_logs", True)
    setup_logging(verbose=args.verbose, json_output=json_logs)
    queues = [q.strip() for q in args.queues.split(",")]
    w = Worker(queues=queues, concurrency=args.concurrency)
    w.start()


def cmd_info(args: argparse.Namespace) -> None:
    if not check_connection():
        print("Cannot connect to Redis")
        sys.exit(1)

    r = get_redis()
    print("Redis: connected")

    workers = cast(set[str], r.smembers(WORKER_REGISTRY))
    alive: list[str] = []
    for wid in workers:
        hb_key = f"{WORKER_HEARTBEAT_PREFIX}:{wid}"
        if r.exists(hb_key):
            alive.append(wid)

    print(f"Workers: {len(alive)} alive / {len(workers)} registered")
    for wid in alive:
        print(f"  - {wid}")

    print(f"\nRegistered task types: {', '.join(list_registered()) or 'none'}")

    all_raw = cast(list[str], r.hvals(TASK_HASH))
    counts: dict[str, int] = {s.value: 0 for s in TaskState}
    for raw in all_raw:
        t = Task.from_json(raw)
        counts[t.state.value] += 1

    print("\nTask counts:")
    for state, count in counts.items():
        if count > 0:
            print(f"  {state}: {count}")

    dead_count = cast(int, r.scard(DEAD_LETTER_SET))
    if dead_count:
        print(f"\nDead letter queue: {dead_count} tasks")


def cmd_purge(args: argparse.Namespace) -> None:
    r = get_redis()
    if args.target == "all":
        keys = cast(list[str], r.keys("dtask:*"))
        if keys:
            r.delete(*keys)
        print(f"Purged {len(keys)} keys")
    elif args.target == "dead":
        dead_ids = cast(set[str], r.smembers(DEAD_LETTER_SET))
        if dead_ids:
            pipe = r.pipeline()
            for tid in dead_ids:
                pipe.hdel(TASK_HASH, tid)
            pipe.delete(DEAD_LETTER_SET)
            pipe.execute()
        print(f"Purged {len(dead_ids)} dead tasks")
    elif args.target == "completed":
        all_raw = cast(list[str], r.hvals(TASK_HASH))
        count = 0
        pipe = r.pipeline()
        for raw in all_raw:
            t = Task.from_json(raw)
            if t.state == TaskState.SUCCESS:
                pipe.hdel(TASK_HASH, t.task_id)
                count += 1
        pipe.execute()
        print(f"Purged {count} completed tasks")


def _fmt_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dtask",
        description="Distributed Task Scheduler backed by Redis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              dtask submit echo '{"message": "hello"}'
              dtask submit flaky_job '{"fail_rate": 0.7}' --retries 5
              dtask submit slow_job '{"duration": 5}' --delay 10
              dtask status <task_id>
              dtask list --state running
              dtask worker --queues default --concurrency 2
              dtask info
              dtask purge dead
        """),
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit", help="Submit a new task")
    p_submit.add_argument("task_type", help="Task type (e.g., echo, add, flaky_job)")
    p_submit.add_argument("payload", nargs="?", default="{}", help="JSON payload")
    p_submit.add_argument("--queue", default="default", help="Target queue (default: default)")
    p_submit.add_argument("--retries", type=int, default=3, help="Max retries (default: 3)")
    p_submit.add_argument("--delay", type=int, default=0, help="Delay before execution in seconds")
    p_submit.set_defaults(func=cmd_submit)

    p_status = sub.add_parser("status", help="Get task status")
    p_status.add_argument("task_id", help="Task ID")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="List tasks")
    p_list.add_argument("--state", help="Filter by state")
    p_list.add_argument("--limit", type=int, default=50, help="Max results")
    p_list.set_defaults(func=cmd_list)

    p_cancel = sub.add_parser("cancel", help="Cancel a pending/queued task")
    p_cancel.add_argument("task_id", help="Task ID")
    p_cancel.set_defaults(func=cmd_cancel)

    p_worker = sub.add_parser("worker", help="Start a worker process")
    p_worker.add_argument("--queues", default="default", help="Comma-separated queue names")
    p_worker.add_argument("--concurrency", type=int, default=1, help="Number of worker threads")
    p_worker.add_argument("-v", "--verbose", action="store_true")
    p_worker.add_argument("--json-logs", dest="json_logs", action="store_true", default=True,
                          help="Output logs as JSON (default)")
    p_worker.add_argument("--no-json-logs", dest="json_logs", action="store_false",
                          help="Output logs as plain text")
    p_worker.set_defaults(func=cmd_worker)

    p_info = sub.add_parser("info", help="Show scheduler info")
    p_info.set_defaults(func=cmd_info)

    p_purge = sub.add_parser("purge", help="Purge tasks")
    p_purge.add_argument("target", choices=["all", "dead", "completed"], help="What to purge")
    p_purge.set_defaults(func=cmd_purge)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)


if __name__ == "__main__":
    main()
