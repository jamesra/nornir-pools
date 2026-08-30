"""Pool shutdown must finish tearing down even when something above it raises.

``MultiprocessThreadPool.shutdown`` was::

    if self._tasks is not None:
        self.wait_completion()
        self._tasks.close()
        self._tasks.join()

        assert (len(self._active_tasks) == 0)
        self._tasks = None

    nornir_pools._remove_pool(self)

The filed concern was that the assert vanishes under ``-O``, and that with assertions on
a task leak crashes teardown. Neither can happen, because ``wait_completion`` is::

    while len(self._active_tasks) > 0:
        pending = list(self._active_tasks.values())
        for task in pending:
            task.wait()

It only returns once ``_active_tasks`` is empty, and ``close()``/``join()`` add nothing,
so the assert could only ever observe 0. Measured with a genuine leak -- a stale entry
whose ``asyncresult`` had already completed, so ``wait()`` returns immediately but
nothing removes the entry -- ``wait_completion`` spun for the full 10 second budget and
never returned. A leak therefore hangs *before* the assert, identically under ``-O``.

So the assert was dead. What was actually fragile is that nothing guaranteed teardown
completed: if ``close()``, ``join()`` or the assert raised, then ``self._tasks = None``
and ``_remove_pool(self)`` were skipped, leaving this pool registered in
``dictKnownPools`` wrapping a closed ``multiprocessing.Pool``. Since pools are cached by
name, the next lookup handed out that dead pool.

Teardown now runs in a ``finally``, and the dead assert became a logged check that
survives ``-O`` and names the offending tasks.

The busy-spin in ``wait_completion`` is the substantive bug this uncovered and is filed
separately; fixing it means giving that loop a bounded wait, which is a material change
to a path whose comments document a race it was tuned around.
"""
from __future__ import annotations

import unittest

import nornir_pools as pools
import nornir_pools._test_pool_tasks as pool_tasks  # pyright: ignore[reportMissingImports]
import nornir_pools.multiprocessthreadpool as multiprocessthreadpool

SquareTheNumber = pool_tasks.SquareTheNumber

# The pool's logger comes from PoolBase, so it is named for that module rather than for
# multiprocessthreadpool. MultiprocessThreadTask has its own logger under the latter.
POOL_LOGGER = 'nornir_pools.poolbase'


class _DirectPoolFixture(unittest.TestCase):
    """Builds pools directly, bypassing the GetMultithreadingPool name cache."""

    def _pool(self, name):
        pool = multiprocessthreadpool.MultiprocessThreadPool(name, 2)
        self.addCleanup(self._force_cleanup, pool)
        return pool

    @staticmethod
    def _force_cleanup(pool):
        pool._active_tasks.clear()
        try:
            pool.terminate_workers()
        except Exception:
            pass
        try:
            pools._remove_pool(pool)
        except Exception:
            pass


class TestNormalShutdown(_DirectPoolFixture):

    def test_shutdown_clears_the_worker_pool(self):
        pool = self._pool('test-shutdown-normal')
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        pool.shutdown()

        self.assertIsNone(pool._tasks)

    def test_shutdown_deregisters_the_pool(self):
        pool = self._pool('test-shutdown-deregister')
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        pool.shutdown()

        self.assertNotIn(pool, pools.dictKnownPools.values())

    def test_shutdown_leaves_no_active_tasks(self):
        pool = self._pool('test-shutdown-drained')
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        pool.shutdown()

        self.assertEqual(len(pool._active_tasks), 0)

    def test_shutdown_on_an_unused_pool_is_safe(self):
        """_tasks is still None, so the guarded block never runs."""
        pool = self._pool('test-shutdown-unused')

        pool.shutdown()

        self.assertIsNone(pool._tasks)

    def test_shutdown_logs_no_error_when_nothing_leaked(self):
        pool = self._pool('test-shutdown-quiet')
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        with self.assertNoLogs(POOL_LOGGER, level='ERROR'):
            pool.shutdown()

    def test_shutdown_is_idempotent(self):
        pool = self._pool('test-shutdown-twice')
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        pool.shutdown()
        pool.shutdown()

        self.assertIsNone(pool._tasks)


class TestTeardownCompletesWhenShutdownRaises(_DirectPoolFixture):
    """The real hazard: a raise used to skip _tasks = None and _remove_pool."""

    def _pool_that_fails_to_close(self, name):
        pool = self._pool(name)
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        def explode():
            raise RuntimeError('close failed')

        pool._tasks.close = explode  # type: ignore[method-assign]
        return pool

    def test_the_exception_still_propagates(self):
        pool = self._pool_that_fails_to_close('test-teardown-raises')

        with self.assertRaises(RuntimeError):
            pool.shutdown()

    def test_the_worker_pool_reference_is_cleared_anyway(self):
        pool = self._pool_that_fails_to_close('test-teardown-clears')

        with self.assertRaises(RuntimeError):
            pool.shutdown()

        self.assertIsNone(pool._tasks,
                          'a failed shutdown must not keep a closed pool around')

    def test_the_pool_is_deregistered_anyway(self):
        pool = self._pool_that_fails_to_close('test-teardown-deregisters')

        with self.assertRaises(RuntimeError):
            pool.shutdown()

        self.assertNotIn(pool, pools.dictKnownPools.values(),
                         'a failed shutdown must not leave a dead pool registered')

    def test_a_registered_pool_is_deregistered_even_when_close_raises(self):
        """Built through the factory so it is genuinely in dictKnownPools.

        The directly-constructed pools above are never registered, so their
        deregistration assertions hold trivially. This one has something to remove.
        """
        name = 'test-teardown-named-lookup'
        pool = pools.GetMultithreadingPool(name, num_threads=2)
        self.addCleanup(self._force_cleanup, pool)
        pool.add_task('t', SquareTheNumber, 5).wait_return()
        self.assertIn(pool, pools.dictKnownPools.values(),
                      'precondition: the factory must have registered this pool')

        def explode():
            raise RuntimeError('close failed')

        pool._tasks.close = explode  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError):
            pool.shutdown()

        self.assertNotIn(pool, pools.dictKnownPools.values(),
                         'a failed shutdown left a closed pool in the name cache, so '
                         'the next lookup by this name handed out a dead pool')

    def test_a_named_lookup_after_a_failed_shutdown_gives_a_fresh_pool(self):
        name = 'test-teardown-fresh-lookup'
        pool = pools.GetMultithreadingPool(name, num_threads=2)
        self.addCleanup(self._force_cleanup, pool)
        pool.add_task('t', SquareTheNumber, 5).wait_return()

        def explode():
            raise RuntimeError('close failed')

        pool._tasks.close = explode  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            pool.shutdown()

        replacement = pools.GetMultithreadingPool(name, num_threads=2)
        self.addCleanup(self._force_cleanup, replacement)

        self.assertIsNot(replacement, pool)
        self.assertEqual(replacement.add_task('t', SquareTheNumber, 6).wait_return(), 36)


class TestLeakIsReportedNotAsserted(_DirectPoolFixture):
    """The check has to survive -O and say which tasks leaked."""

    def test_a_leak_discovered_during_shutdown_is_logged(self):
        pool = self._pool('test-leak-logged')
        task = pool.add_task('t', SquareTheNumber, 5)
        task.wait_return()

        # Re-register after completion so the leak check has something to find without
        # making wait_completion spin: patch it out, since its busy-spin on a stale
        # entry is the separately filed bug.
        pool.wait_completion = lambda: None  # type: ignore[method-assign]
        pool._active_tasks[task.task_id] = task

        with self.assertLogs(POOL_LOGGER, level='ERROR') as captured:
            pool.shutdown()

        self.assertTrue(any('still registered as active' in m for m in captured.output),
                        f'expected a leak report, got {captured.output}')

    def test_the_leak_report_names_the_task_id(self):
        pool = self._pool('test-leak-names-id')
        task = pool.add_task('t', SquareTheNumber, 5)
        task.wait_return()
        pool.wait_completion = lambda: None  # type: ignore[method-assign]
        pool._active_tasks[task.task_id] = task

        with self.assertLogs(POOL_LOGGER, level='ERROR') as captured:
            pool.shutdown()

        self.assertTrue(any(str(task.task_id) in m for m in captured.output))

    def test_a_leak_does_not_abort_teardown(self):
        pool = self._pool('test-leak-still-tears-down')
        task = pool.add_task('t', SquareTheNumber, 5)
        task.wait_return()
        pool.wait_completion = lambda: None  # type: ignore[method-assign]
        pool._active_tasks[task.task_id] = task

        with self.assertLogs(POOL_LOGGER, level='ERROR'):
            pool.shutdown()

        self.assertIsNone(pool._tasks)
        self.assertEqual(len(pool._active_tasks), 0)

    def test_the_check_does_not_depend_on_assertions(self):
        """A bare assert would compile away under -O; this must not."""
        import inspect

        source = inspect.getsource(
            multiprocessthreadpool.MultiprocessThreadPool.shutdown)
        code = '\n'.join(line.split('#', 1)[0] for line in source.splitlines())

        self.assertNotIn('assert', code)


class TestWaitCompletionPostcondition(_DirectPoolFixture):
    """Documents why the old assert was unreachable."""

    def test_wait_completion_only_returns_when_no_tasks_are_active(self):
        pool = self._pool('test-waitcompletion-drains')
        for i in range(8):
            pool.add_task(str(i), SquareTheNumber, i)

        pool.wait_completion()

        self.assertEqual(len(pool._active_tasks), 0,
                         'shutdown asserted this immediately afterward, so it could '
                         'never have observed a nonzero count')


if __name__ == '__main__':
    unittest.main()
