"""Tests for the task handler registry."""

from dtask.registry import register, get_handler, list_registered, _handlers


class TestRegistry:
    def setup_method(self):
        _handlers.clear()

    def test_register_and_get(self):
        @register("my_task")
        def handler(payload):
            return payload

        assert get_handler("my_task") is handler

    def test_get_missing_handler(self):
        assert get_handler("nonexistent") is None

    def test_list_registered(self):
        @register("task_a")
        def a(p):
            return p

        @register("task_b")
        def b(p):
            return p

        registered = list_registered()
        assert "task_a" in registered
        assert "task_b" in registered

    def test_handler_executes(self):
        @register("adder")
        def adder(payload):
            return {"sum": payload["a"] + payload["b"]}

        h = get_handler("adder")
        assert h is not None
        result = h({"a": 10, "b": 20})
        assert result == {"sum": 30}
