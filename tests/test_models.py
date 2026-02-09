"""Tests for task models and state machine."""

from dtask.models import Task, TaskState


class TestTask:
    def test_create_task_defaults(self):
        t = Task(task_type="echo", payload={"msg": "hi"})
        assert t.task_type == "echo"
        assert t.payload == {"msg": "hi"}
        assert t.state == TaskState.PENDING
        assert t.attempts == 0
        assert t.max_retries == 3
        assert t.queue == "default"
        assert len(t.task_id) == 12

    def test_serialization_roundtrip(self):
        t = Task(task_type="add", payload={"a": 1, "b": 2}, max_retries=5)
        t.state = TaskState.RUNNING
        t.attempts = 2

        json_str = t.to_json()
        restored = Task.from_json(json_str)

        assert restored.task_type == "add"
        assert restored.payload == {"a": 1, "b": 2}
        assert restored.state == TaskState.RUNNING
        assert restored.attempts == 2
        assert restored.max_retries == 5
        assert restored.task_id == t.task_id

    def test_all_states_serializable(self):
        for state in TaskState:
            t = Task(task_type="test", payload={})
            t.state = state
            restored = Task.from_json(t.to_json())
            assert restored.state == state

    def test_touch_updates_timestamp(self):
        t = Task(task_type="test", payload={})
        old_ts = t.updated_at
        import time
        time.sleep(0.01)
        t.touch()
        assert t.updated_at > old_ts

    def test_result_serialization(self):
        t = Task(task_type="test", payload={})
        t.result = {"answer": 42, "nested": [1, 2, 3]}
        restored = Task.from_json(t.to_json())
        assert restored.result == {"answer": 42, "nested": [1, 2, 3]}

    def test_error_serialization(self):
        t = Task(task_type="test", payload={})
        t.error = "RuntimeError: something broke\nTraceback..."
        restored = Task.from_json(t.to_json())
        assert restored.error == "RuntimeError: something broke\nTraceback..."
