"""Regression for #176: Task.__str__ time column alignment."""
from __future__ import annotations

import unittest

from nornir_pools.task import Task


class _StubTask(Task):
    def wait(self):
        return None

    def wait_return(self):
        return None

    def iscompleted(self) -> bool:
        return True


class TestTaskStrAlignment(unittest.TestCase):
    def test_elapsed_time_starts_at_column_70(self) -> None:
        """#176: pad against the name prefix length, not len(time_str)."""
        task = _StubTask('short-name')
        task.set_completion_time()
        text = str(task)
        time_str = task.elapsed_time_str
        self.assertTrue(text.endswith(time_str))
        self.assertEqual(len(text) - len(time_str), 70)

    def test_long_name_does_not_use_negative_pad(self) -> None:
        task = _StubTask('x' * 80)
        task.set_completion_time()
        text = str(task)
        time_str = task.elapsed_time_str
        self.assertTrue(text.endswith(time_str))
        # Name already past column 70: no pad, time follows immediately.
        self.assertEqual(text, f"--- {'x' * 80}{time_str}")


if __name__ == '__main__':
    unittest.main()
