# Distributed Task Scheduler (dtask)

## Overview
A production-grade Python distributed task scheduler backed by a single Redis instance. CLI-driven, focused on correctness, retries, and fault tolerance.

## Architecture

### High-Level Design
```
CLI (submit/status/list/cancel/purge/info)
    |
    v
Producer --> Redis Queues --> Worker(s) --> Task Handlers
                |                |
          Scheduled Set     Processing List
                |                |
          Dead Letter Set   Heartbeat Keys
```

### Core Components
- **Producer** (`src/dtask/producer.py`): Enqueues tasks, manages delayed scheduling
- **Worker** (`src/dtask/worker.py`): Consumes tasks with concurrency, heartbeats, dead worker reaping
- **Registry** (`src/dtask/registry.py`): Maps task_type strings to handler functions via decorators
- **Models** (`src/dtask/models.py`): Task dataclass with full lifecycle state machine
- **Logging** (`src/dtask/logging.py`): Structured JSON log formatter with task/worker context fields
- **CLI** (`src/dtask/cli.py`): argparse-based command interface

### Redis Data Structures
| Key | Type | Purpose |
|-----|------|---------|
| `dtask:task` | Hash | task_id -> JSON task data |
| `dtask:queue:{name}` | List | FIFO queue per queue name |
| `dtask:processing:{worker_id}` | List | Tasks currently being processed |
| `dtask:scheduled` | Sorted Set | Delayed tasks (score = execute_at timestamp) |
| `dtask:dead` | Set | Dead-lettered task IDs |
| `dtask:workers` | Set | Registered worker IDs |
| `dtask:worker:heartbeat:{id}` | String (TTL) | Worker liveness (auto-expires) |

### Task Lifecycle
`PENDING -> QUEUED -> RUNNING -> SUCCESS`
`RUNNING -> RETRYING -> QUEUED -> ... -> DEAD`

### Failure & Retry Strategy
- Exponential backoff: `delay * 2^(attempt-1)`
- Configurable max retries per task
- Dead letter set for exhausted retries
- Worker heartbeat with TTL-based expiry
- Orphan task reaping from dead workers
- Graceful shutdown re-queues in-flight tasks

### What NOT Built (by design)
- No web UI
- No task dependencies/DAGs
- No priority queues
- No result backend (results stored in task hash)
- No multi-Redis / cluster support
- No rate limiting
- No task deduplication
- No persistent task history / TTL cleanup

## Project Structure
```
src/dtask/
  __init__.py        - Package init
  config.py          - Redis connection config, key prefixes
  connection.py      - Redis connection pool
  models.py          - Task dataclass and state enum
  producer.py        - Task enqueueing and query functions
  registry.py        - Handler registration decorator
  logging.py         - Structured JSON log formatter
  lua_scripts.py     - Lua scripts for atomic Redis operations
  worker.py          - Worker with concurrency, heartbeat, reaping
  tasks.py           - Example task handlers (echo, add, send_email, cpu_work, always_fail, etc.)
  cli.py             - CLI interface
tests/
  conftest.py        - Shared fixtures (Redis flush, client)
  test_models.py     - Task model & serialization tests
  test_registry.py   - Handler registration tests
  test_producer.py   - Enqueue, list, cancel tests
  test_worker.py     - Worker execution, retries, concurrency tests
main.py              - Entry point
demo.py              - Interactive demo script
pyproject.toml       - Project metadata, CLI entrypoint, pytest config
```

## Usage
```bash
# Submit tasks
python main.py submit echo '{"message": "hello"}'
python main.py submit add '{"a": 10, "b": 20}'
python main.py submit flaky_job '{"fail_rate": 0.5}' --retries 5
python main.py submit slow_job '{"duration": 3}' --delay 10

# Start a worker
python main.py worker --queues default --concurrency 2

# Inspect
python main.py list
python main.py status <task_id>
python main.py info

# Manage
python main.py cancel <task_id>
python main.py purge dead
python main.py purge completed
python main.py purge all
```

## Configuration
- Redis URL: `REDIS_URL` env var (default: `redis://localhost:6000/0`)
- Redis runs on port 6000 in this environment

## Testing
```bash
python -m pytest tests/ -v
```
28 tests covering models, registry, producer, and worker.

## Recent Changes
- 2026-02-09: Added structured JSON logging with task lifecycle events, worker events, and failure context
- 2026-02-09: Added send_email, cpu_work, and always_fail example task handlers
- 2026-02-09: Added tests, proper pyproject.toml with CLI entrypoint, type annotations, Lua scripts for atomic operations
- 2026-02-09: Initial implementation of full task scheduler
