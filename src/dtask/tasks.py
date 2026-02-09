"""Example task handlers demonstrating the registry pattern."""

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
