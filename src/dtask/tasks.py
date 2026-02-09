"""Example task handlers demonstrating the registry pattern."""

import hashlib
import random
import time

from .registry import register


@register("echo")
def handle_echo(payload: dict) -> dict:
    message = payload.get("message", "")
    return {"echoed": message, "timestamp": time.time()}


@register("add")
def handle_add(payload: dict) -> dict:
    a = payload.get("a", 0)
    b = payload.get("b", 0)
    return {"result": a + b}


@register("slow_job")
def handle_slow_job(payload: dict) -> dict:
    duration = payload.get("duration", 3)
    time.sleep(duration)
    return {"slept_for": duration}


@register("flaky_job")
def handle_flaky_job(payload: dict) -> dict:
    fail_rate = payload.get("fail_rate", 0.5)
    if random.random() < fail_rate:
        raise RuntimeError(f"Simulated failure (fail_rate={fail_rate})")
    return {"status": "survived", "fail_rate": fail_rate}


@register("divide")
def handle_divide(payload: dict) -> dict:
    a = payload.get("a", 10)
    b = payload.get("b", 0)
    return {"result": a / b}


@register("send_email")
def handle_send_email(payload: dict) -> dict:
    to = payload.get("to", "")
    subject = payload.get("subject", "(no subject)")
    body = payload.get("body", "")

    if not to:
        raise ValueError("Missing required field: 'to'")
    if "@" not in to:
        raise ValueError(f"Invalid email address: '{to}'")

    time.sleep(random.uniform(0.5, 2.0))

    message_id = hashlib.sha256(f"{to}:{subject}:{time.time()}".encode()).hexdigest()[:16]

    return {
        "message_id": message_id,
        "to": to,
        "subject": subject,
        "status": "delivered",
        "delivered_at": time.time(),
    }


@register("cpu_work")
def handle_cpu_work(payload: dict) -> dict:
    iterations = payload.get("iterations", 1_000_000)
    algorithm = payload.get("algorithm", "hash")

    start = time.time()

    if algorithm == "hash":
        data = b"dtask-benchmark"
        for i in range(iterations):
            data = hashlib.sha256(data).digest()
        result_hash = data.hex()[:16]
    elif algorithm == "prime_sieve":
        limit = min(iterations, 10_000_000)
        sieve = bytearray(b'\x01') * (limit + 1)
        sieve[0] = sieve[1] = 0
        for i in range(2, int(limit**0.5) + 1):
            if sieve[i]:
                sieve[i*i::i] = bytearray(len(sieve[i*i::i]))
        prime_count = sum(sieve)
        result_hash = str(prime_count)
    else:
        total = 0.0
        for i in range(1, iterations + 1):
            total += 1.0 / (i * i)
        result_hash = f"{total:.10f}"

    elapsed = time.time() - start

    return {
        "algorithm": algorithm,
        "iterations": iterations,
        "elapsed_sec": round(elapsed, 4),
        "result": result_hash,
    }


@register("always_fail")
def handle_always_fail(payload: dict) -> dict:
    error_type = payload.get("error_type", "runtime")
    message = payload.get("message", "This task always fails for retry demonstration")

    if error_type == "value":
        raise ValueError(message)
    elif error_type == "timeout":
        raise TimeoutError(message)
    elif error_type == "io":
        raise IOError(message)
    else:
        raise RuntimeError(message)
