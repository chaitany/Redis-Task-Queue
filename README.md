# dtask — Distributed Task Scheduler

A distributed task scheduler backed by a single Redis instance. Built for correctness over complexity: atomic state transitions via Lua scripts, fault-tolerant workers with heartbeat-based failure detection, and exponential backoff retries with dead-letter semantics. CLI-driven, no web UI.

## Problem Statement

Background job processing in distributed systems requires solving several hard problems simultaneously: exactly-once delivery semantics, worker crash recovery, retry strategies that don't amplify failures, and visibility into task state across the system. Most production solutions (Celery, RQ, Sidekiq) bring significant operational overhead.

dtask solves these problems with a minimal, single-Redis design that's small enough to audit end-to-end, while implementing the fault tolerance mechanisms that actually matter: atomic claim/complete/fail transitions, heartbeat-based dead worker detection, and orphan task recovery.

## Architecture

```
                         ┌──────────────────────────────┐
                         │           Redis              │
                         │                              │
  CLI ──► Producer ─────►│  dtask:queue:{name}   (List) │──► Worker Thread 1 ──► Handler
                         │  dtask:task           (Hash) │──► Worker Thread 2 ──► Handler
                         │  dtask:scheduled      (ZSet) │──► Worker Thread N ──► Handler
                         │  dtask:processing:{w} (List) │
                         │  dtask:dead           (Set)  │    ┌─────────────────┐
                         │  dtask:workers        (Set)  │◄───│ Heartbeat Loop  │
                         │  dtask:worker:hb:{w}  (TTL)  │    │ Scheduler Loop  │
                         └──────────────────────────────┘    │ Reaper Loop     │
                                                             └─────────────────┘
```

The design is deliberately single-writer-per-state-transition. Every mutation to task state flows through a Lua script that reads current state, validates the transition, writes new state, and updates the relevant data structures — all as a single atomic Redis operation. No WATCH/MULTI, no optimistic locking, no retry loops on the client side.

Workers are stateless processes. They register with Redis, maintain a heartbeat, and pull work from queues. If a worker dies, any surviving worker detects the missing heartbeat and recovers its in-flight tasks.

## Task Lifecycle

```
  submit(delay=0)          claim              handler returns
      │                      │                      │
      ▼                      ▼                      ▼
   PENDING ──► QUEUED ──► RUNNING ──────────────► SUCCESS
      │                      │
      │ (delay>0)            │ handler raises
      │                      ▼
      │                   RETRYING ──► (backoff) ──► QUEUED ──► RUNNING ──► ...
      │                      │
      │                      │ retries exhausted
      │                      ▼
      │                    DEAD
      │
      └──► (scheduler promotes when time arrives) ──► QUEUED
```

Seven states, no ambiguity. Each transition is enforced atomically in Redis — a task cannot be in an inconsistent state even under concurrent access from multiple workers.

## Redis Schema

| Key Pattern | Type | Purpose |
|---|---|---|
| `dtask:task` | Hash | Canonical task store. `HGET task_id` returns full JSON: state, payload, result, error, timestamps, retry config. Single source of truth. |
| `dtask:queue:{name}` | List | FIFO work queue. `RPUSH` to enqueue, `LPOP` to dequeue. Multiple named queues supported. |
| `dtask:processing:{worker_id}` | List | Per-worker in-flight task list. If the worker dies, another worker reads this list and re-queues the tasks. |
| `dtask:scheduled` | Sorted Set | Delayed and retry tasks. Score = Unix timestamp when the task should become eligible. Scheduler loop runs `ZRANGEBYSCORE ... -inf <now>` every 2s. |
| `dtask:dead` | Set | Task IDs that exhausted all retries. Inspectable via CLI, purgeable. |
| `dtask:workers` | Set | All registered worker IDs. Compared against heartbeat keys to detect dead workers. |
| `dtask:worker:heartbeat:{id}` | String + TTL | `SETEX` with 30s TTL, refreshed every 5s. If the key expires, the worker is considered dead. |

### Why These Structures

**Lists for queues** — `RPUSH`/`LPOP` gives O(1) FIFO. `BLPOP` gives zero-CPU blocking waits (though we use `RPOP` + sleep to check the shutdown flag between polls).

**Sorted set for scheduling** — Exponential backoff means each retry needs a different delay. The score is the "execute at" timestamp, so `ZRANGEBYSCORE` efficiently finds all ready tasks. This also handles initial delayed submissions — same mechanism, no special case.

**Per-worker processing lists** — A global processing set would require scanning all in-flight tasks to find which belong to a dead worker. Per-worker lists make recovery O(tasks-per-worker) instead of O(total-in-flight).

**Hash for task data** — O(1) lookups by task ID. A single hash instead of per-task keys avoids key proliferation and makes `HVALS` scans for listing practical at moderate scale.

## Failure Handling

### Retry with Exponential Backoff

When a handler raises an exception:

1. The `FAIL_TASK_SCRIPT` Lua script runs atomically
2. If `attempts <= max_retries`: state → `RETRYING`, task added to scheduled set with delay `base_delay * 2^(attempt-1)`
3. If retries exhausted: state → `DEAD`, task added to dead-letter set
4. Task removed from worker's processing list in either case

Backoff schedule with `retry_delay_sec=5`:

| Attempt | Delay | Cumulative |
|---|---|---|
| 1 | 5s | 5s |
| 2 | 10s | 15s |
| 3 | 20s | 35s |
| 4 | 40s | 75s |
| 5 | 80s | 155s |

### Worker Crash Recovery

```
T+0s     Worker A claims task, begins execution
T+3s     Worker A crashes (kill -9, OOM, segfault)
T+30s    Heartbeat key auto-expires in Redis (TTL)
T+30-45s Worker B's reaper loop detects missing heartbeat
T+45s    Worker B reads Worker A's processing list
T+45s    Worker B re-queues each orphaned task via Lua script
T+45s    Task resumes on next available worker
```

Worst-case recovery: ~45 seconds (30s heartbeat TTL + 15s reaper interval). The Lua requeue script checks task state before re-queuing — tasks that completed between the crash and reaping are not duplicated.

### Graceful Shutdown

On SIGTERM/SIGINT:
1. Shutdown event is set, all loops exit
2. Worker threads are joined (10s timeout)
3. Worker unregisters from registry, deletes heartbeat key
4. Any remaining tasks in the processing list are re-queued
5. Processing list is cleaned up

No task is lost in any shutdown scenario.

## Structured Logging

All worker events emit structured JSON to stderr:

```json
{"ts": "2026-02-09T14:45:58", "level": "INFO", "event": "task_start", "task_id": "35e40958fd47", "task_type": "echo", "worker_id": "worker-1613-97061b", "queue": "default", "attempt": 1, "max_attempts": 4}
{"ts": "2026-02-09T14:45:58", "level": "INFO", "event": "task_success", "task_id": "35e40958fd47", "task_type": "echo", "duration_sec": 0.0012, "attempt": 1}
{"ts": "2026-02-09T14:46:00", "level": "WARNING", "event": "task_retry", "task_id": "061c52579a25", "task_type": "always_fail", "delay_sec": 10, "error_type": "RuntimeError", "error_msg": "service unreachable", "attempt": 2, "max_attempts": 4}
{"ts": "2026-02-09T14:46:20", "level": "ERROR", "event": "task_dead", "task_id": "061c52579a25", "task_type": "always_fail", "error_type": "RuntimeError", "attempt": 4, "max_attempts": 4, "outcome": "dead"}
```

Every log line includes `worker_id` for multi-worker correlation. Failure logs include `error_type`, `error_msg`, `attempt`, `delay_sec`, and `outcome`. Use `--no-json-logs` for human-readable output during development.

## Usage

```bash
# Submit tasks
dtask submit echo '{"message": "hello"}'
dtask submit send_email '{"to": "user@example.com", "subject": "Hello"}' --retries 5
dtask submit cpu_work '{"iterations": 1000000, "algorithm": "prime_sieve"}'
dtask submit always_fail '{}' --retries 3          # retry demo

# Delayed execution
dtask submit slow_job '{"duration": 5}' --delay 30

# Start workers
dtask worker --queues default --concurrency 4
dtask worker --queues default --concurrency 2 --no-json-logs  # plain text logs

# Inspect
dtask list
dtask list --state dead
dtask status <task_id>
dtask info

# Manage
dtask cancel <task_id>
dtask purge dead
dtask purge completed
dtask purge all
```

## Testing

```bash
python -m pytest tests/ -v
```

28 tests covering models (serialization, state machine), registry (handler registration), producer (enqueue, cancel, list), and worker (execution, retries, dead-lettering, delayed promotion, concurrency). All worker tests run against real Redis — no mocks.

## Trade-offs

**Threading over multiprocessing/asyncio.** Task handlers are assumed I/O-bound. Threads share the shutdown event trivially. The GIL is released during Redis calls and network I/O, so concurrency is real. CPU-bound handlers will bottleneck on the GIL — for those workloads, run multiple worker processes instead of threads.

**Polling over blocking.** Workers use `RPOP` + 1s sleep instead of `BLPOP`. This is slightly less efficient but allows checking the shutdown flag between polls. At moderate throughput (<1000 tasks/sec), the difference is negligible.

**Single hash for all tasks.** `HVALS` scans the entire hash for listing. Fine for thousands of tasks, problematic at millions. A production system would add secondary indexes or paginated cursors.

**No task deduplication.** Submitting the same task twice creates two independent tasks. Idempotency is the handler's responsibility.

**Per-task retry config over global policy.** Each task carries its own `max_retries` and `retry_delay_sec`. More flexible, but means the producer must specify policy at submit time.

**Lua scripts over WATCH/MULTI.** Lua is more complex to write but eliminates retry loops on contention. Every state transition is a single round-trip regardless of concurrent access.

## What Would Be Added in Production

**Task TTL / cleanup.** Completed and dead tasks accumulate indefinitely. A production system needs a reaper that expires old task data after a configurable retention period, or moves it to cold storage.

**Priority queues.** Currently all tasks in a queue are FIFO. Real workloads need priority lanes — implementable with multiple queues polled in priority order, or a sorted set keyed by priority score.

**Rate limiting.** Per-task-type or per-queue rate limiting to prevent downstream service overload. Token bucket in Redis, checked before dispatch.

**Task dependencies / DAGs.** Workflows where task B runs only after task A completes. Requires a dependency graph and a trigger mechanism.

**Result backend.** Results are stored in the task hash, which works for small payloads. Large results should go to object storage with a reference in the hash.

**Observability.** Prometheus metrics (queue depth, processing latency, retry rate, dead-letter rate), distributed tracing (OpenTelemetry spans per task execution), and alerting on dead-letter queue growth.

**Multi-tenancy.** Key prefix per tenant, queue isolation, per-tenant rate limits.

**Persistence guarantees.** Redis AOF/RDB for crash recovery of the Redis instance itself. For stronger durability, a write-ahead log to PostgreSQL before enqueueing.

**Horizontal scaling.** Redis Cluster or sharding by queue name for throughput beyond single-instance limits. Requires consistent hashing for task-to-shard mapping.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6000/0` | Redis connection string |
| Heartbeat interval | 5s | How often workers refresh their heartbeat |
| Heartbeat timeout | 30s | TTL on heartbeat keys; dead after expiry |
| Reaper interval | 15s | How often workers check for dead peers |
| Default max retries | 3 | Per-task, overridable at submit time |
| Default retry delay | 5s | Base delay for exponential backoff |
| Default timeout | 60s | Per-task execution timeout |
