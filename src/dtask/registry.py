"""Task handler registry - maps task_type strings to callable handlers."""

from typing import Callable, Any

_handlers: dict[str, Callable[..., Any]] = {}


def register(task_type: str):
    def decorator(fn: Callable[..., Any]):
        _handlers[task_type] = fn
        return fn
    return decorator


def get_handler(task_type: str) -> Callable[..., Any] | None:
    return _handlers.get(task_type)


def list_registered() -> list[str]:
    return list(_handlers.keys())
