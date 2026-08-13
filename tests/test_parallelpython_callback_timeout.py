"""Unit tests for ParallelPython CTask wait timeout / ActiveJobCount unwind."""

from __future__ import annotations

import unittest

import nornir_pools.parallelpythonpool as pp


class _FakeServer:
    def wait(self, groupname) -> None:
        return None


class TestParallelPythonCallbackTimeout(unittest.TestCase):
    """Bounded wait + ActiveJobCount release when PP callback never fires."""

    def setUp(self) -> None:
        self._prev_count = pp.ActiveJobCount
        pp.ActiveJobCount = 0

    def tearDown(self) -> None:
        pp.ActiveJobCount = self._prev_count

    def test_missing_callback_unwinds_job_count_and_raises(self) -> None:
        pp.IncrementActiveJobCount()
        self.assertEqual(pp.ActiveJobCount, 1)

        task = pp.CTask(_FakeServer(), "group-test", name="hung")
        task.PRIMARY_CALLBACK_TIMEOUT_S = 0.01
        task.SECONDARY_CALLBACK_TIMEOUT_S = 0.01

        with self.assertRaises(RuntimeError):
            task.wait()

        self.assertEqual(pp.ActiveJobCount, 0)
        self.assertTrue(task._job_count_released)
        self.assertTrue(task.completed.is_set())

    def test_late_callback_does_not_double_decrement(self) -> None:
        pp.IncrementActiveJobCount()
        task = pp.CTask(_FakeServer(), "group-test", name="late")
        task.PRIMARY_CALLBACK_TIMEOUT_S = 0.01
        task.SECONDARY_CALLBACK_TIMEOUT_S = 0.01

        with self.assertRaises(RuntimeError):
            task.wait()
        self.assertEqual(pp.ActiveJobCount, 0)

        # A delayed callback must not drive the counter negative.
        task.callback({"returncode": 0, "returned_value": None})
        self.assertEqual(pp.ActiveJobCount, 0)
        self.assertTrue(task._callback_reached)

    def test_successful_callback_still_decrements_once(self) -> None:
        pp.IncrementActiveJobCount()
        task = pp.CTask(_FakeServer(), "group-ok", name="ok")
        task.callback({"returncode": 0, "returned_value": 42, "stdoutdata": "42"})
        self.assertEqual(pp.ActiveJobCount, 0)
        self.assertEqual(task.wait_return(), "42")
        # Second wait must not decrement again.
        task.wait()
        self.assertEqual(pp.ActiveJobCount, 0)


if __name__ == "__main__":
    unittest.main()
