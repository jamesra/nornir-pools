"""SerialPool reports a task's exception from wait_return(), like the thread and process pools (C13-B010)."""
import unittest

import nornir_pools


def _fail(message: str):
    raise ValueError(message)


def _double(value: int) -> int:
    return value * 2


class TestSerialPoolTaskExceptions(unittest.TestCase):

    def test_exception_is_raised_from_wait_return_not_add_task(self):
        pool = nornir_pools.GetGlobalSerialPool()
        task = pool.add_task('fails', _fail, 'Entire image is masked')
        with self.assertRaisesRegex(ValueError, 'Entire image is masked'):
            task.wait_return()

    def test_later_tasks_still_run_after_a_failure(self):
        pool = nornir_pools.GetGlobalSerialPool()
        tasks = [pool.add_task('fails', _fail, 'x'), pool.add_task('ok', _double, 21)]
        pool.wait_completion()
        self.assertEqual(tasks[1].wait_return(), 42)
        self.assertEqual(tasks[0].returncode, 1)

    def test_successful_task_returns_value(self):
        task = nornir_pools.GetGlobalSerialPool().add_task('ok', _double, 4)
        self.assertEqual(task.wait_return(), 8)
        self.assertEqual(task.returncode, 0)


if __name__ == '__main__':
    unittest.main()
