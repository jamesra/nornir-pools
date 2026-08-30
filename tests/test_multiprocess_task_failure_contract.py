"""wait() and wait_return() must both re-raise a worker failure, and both must log it.

The filed concern was that ``wait_return`` returns ``None`` on failure while ``wait``
re-raises, so callers silently treat failures as empty results. Measured, that does not
happen -- both re-raise:

    wait_return() on failure           raised ProbeError: worker exploded
    wait() on failure                  raised ProbeError: worker exploded

The reason is that ``AsyncResult.get()`` re-raises the worker's exception, and
``wait_return`` called it on its first line::

    retval = self.asyncresult.get()      # raises here for a failed task
    if self.asyncresult.successful():
        return retval
    else:
        self.logger.error(...)           # unreachable
        return None                      # unreachable

A failed task never reached the guard, so the whole ``else`` was dead. Two real
consequences, which is why this was worth changing rather than just closing:

1. The error log never fired for ``wait_return``, while ``wait`` did emit one. That is a
   genuine parity gap, just in logging rather than in raising.
2. ``return None`` advertised a contract the code could not deliver. It is what the
   finding was read off, and it invites callers to write ``if result is None`` checks
   that can never fire.

The failure tail now lives in an ``except`` block that logs and re-raises. Raising
behaviour is unchanged; the log is new. ``wait`` keeps its structure, with the
unreachable trailing ``return None`` dropped.

``test_nornir_pools.VerifyExceptionBehaviour`` already covers that both methods raise.
These tests add the logging parity, pin that ``wait_return`` does not return ``None`` on
failure so the filed symptom cannot appear later, and pin the abstract ``Task`` docstring
contract that both methods promise.
"""
from __future__ import annotations

import unittest

import nornir_pools as pools
import nornir_pools._test_pool_tasks as pool_tasks  # pyright: ignore[reportMissingImports]
import nornir_pools.task

LOGGER_NAME = 'nornir_pools.multiprocessthreadpool'
POOL_NAME = 'Test failure contract pool'

# Worker callables must live in an importable package module, not in tests.*, because
# pool workers start a fresh interpreter and unpickle them by module path.
RaiseException = pool_tasks.RaiseException
IntentionalPoolException = pool_tasks.IntentionalPoolException
SquareTheNumber = pool_tasks.SquareTheNumber


def tearDownModule():
    # GetMultithreadingPool caches by name, so this pool is shared by every test here.
    # Shutting it down per test would close it for the tests that follow.
    pools.ClosePools()


class TestDocumentedContract(unittest.TestCase):

    def test_both_methods_document_that_they_re_raise(self):
        for name in ('wait', 'wait_return'):
            with self.subTest(method=name):
                doc = getattr(nornir_pools.task.Task, name).__doc__ or ''

                self.assertIn('re-raised', doc)


class _PoolFixture(unittest.TestCase):

    def setUp(self):
        self.pool = pools.GetMultithreadingPool(POOL_NAME, num_threads=2)

    def _failing_task(self, name):
        return self.pool.add_task(name, RaiseException, name)

    def _succeeding_task(self, name):
        return self.pool.add_task(name, SquareTheNumber, 7)


class TestFailuresAreRaisedByBoth(_PoolFixture):

    def test_wait_return_raises_the_worker_exception(self):
        task = self._failing_task('wr-raises')

        with self.assertRaises(IntentionalPoolException):
            task.wait_return()

    def test_wait_raises_the_worker_exception(self):
        task = self._failing_task('w-raises')

        with self.assertRaises(IntentionalPoolException):
            task.wait()

    def test_wait_return_does_not_return_none_on_failure(self):
        """The filed symptom. Pinned so it cannot appear later."""
        task = self._failing_task('wr-not-none')

        try:
            result = task.wait_return()
        except IntentionalPoolException:
            return  # correct: it raised

        self.fail(f'wait_return swallowed the failure and returned {result!r}')

    def test_a_second_wait_return_still_raises(self):
        """Callers that retry must not get a silent None the second time."""
        task = self._failing_task('wr-twice')

        with self.assertRaises(IntentionalPoolException):
            task.wait_return()
        with self.assertRaises(IntentionalPoolException):
            task.wait_return()


class TestFailuresAreLoggedByBoth(_PoolFixture):
    """The real parity gap: wait_return's error log was unreachable."""

    def test_wait_return_logs_the_failure(self):
        task = self._failing_task('wr-logs')

        with self.assertLogs(LOGGER_NAME, level='ERROR') as captured:
            with self.assertRaises(IntentionalPoolException):
                task.wait_return()

        self.assertTrue(any('not successful' in m for m in captured.output),
                        f'expected a failure log, got {captured.output}')

    def test_wait_logs_the_failure(self):
        task = self._failing_task('w-logs')

        with self.assertLogs(LOGGER_NAME, level='ERROR') as captured:
            with self.assertRaises(IntentionalPoolException):
                task.wait()

        self.assertTrue(any('not successful' in m for m in captured.output))

    def test_the_log_names_the_task(self):
        task = self._failing_task('a-distinctive-task-name')

        with self.assertLogs(LOGGER_NAME, level='ERROR') as captured:
            with self.assertRaises(IntentionalPoolException):
                task.wait_return()

        self.assertTrue(any('a-distinctive-task-name' in m for m in captured.output))


class TestSuccessPathsAreUnchanged(_PoolFixture):

    def test_wait_return_returns_the_value(self):
        task = self._succeeding_task('wr-ok')

        self.assertEqual(task.wait_return(), 49)

    def test_wait_returns_none_on_success(self):
        task = self._succeeding_task('w-ok')

        self.assertIsNone(task.wait())

    def test_a_successful_task_logs_no_error(self):
        task = self._succeeding_task('w-ok-quiet')

        with self.assertNoLogs(LOGGER_NAME, level='ERROR'):
            task.wait_return()

    def test_a_successful_task_reports_completed(self):
        task = self._succeeding_task('w-ok-complete')
        task.wait()

        self.assertTrue(task.iscompleted)


if __name__ == '__main__':
    unittest.main()
