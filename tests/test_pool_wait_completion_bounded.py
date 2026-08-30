"""``wait_completion`` must always terminate, and a pool callback must never raise.

Two defects, one root cause. ``wait_completion`` was::

    while len(self._active_tasks) > 0:
        pending = list(self._active_tasks.values())
        for task in pending:
            task.wait()

Entries left ``_active_tasks`` only via ``callback_wrapper``, so a task whose callback
never fired made this loop non-terminating. In the shape that matters it was a *hot* spin:
the ``AsyncResult`` had already completed, so ``task.wait()`` returned immediately and the
``while`` re-entered, burning a core until the process was killed. ``shutdown`` calls
``wait_completion`` first, so this hung teardown and ``ClosePools()`` with it.

The loop could not simply pop entries before waiting, and the reason is the second defect.
``callback_wrapper`` raised ``ValueError`` for a ``task_id`` it did not recognise. Measured
on CPython 3.14, raising from a pool callback is far worse than the old comment suggested::

    multiprocessing/pool.py, ApplyResult._set:
        if self._callback and self._success:
            self._callback(self._value)      # <-- our wrapper
        ...
        self._event.set()                    # <-- never reached if it raised

    multiprocessing/pool.py, _handle_results:
        cache[job]._set(i, obj)              # only KeyError is caught

so a raising callback:

  * strands its own task -- ``ready()`` stays ``False``, ``wait()`` blocks forever
  * kills the ``_handle_results`` thread, stranding *every later task* in the pool
  * makes ``pool.terminate()`` fail with "Cannot have cache with result_handler not
    alive", disabling ``terminate_workers()``, the forced escape hatch

Probed directly: after one raising callback, a subsequently submitted healthy task never
became ready and ``terminate()`` raised. So the fix order is the reverse of the obvious
one -- make the callback incapable of raising, and reaping in ``wait_completion`` becomes
safe, which is what lets the loop guarantee progress without any timing heuristic.

The bounded ``timeout=`` is the secondary escape, for the shape where the ``AsyncResult``
genuinely never completes (the worker-side import failure that ``add_task``'s own comment
describes). It is opt-in: from the parent, a task whose callback will never fire is
indistinguishable from a worker that is merely busy, so a finite default would eventually
kill legitimate long-running work.

Review issue #221 (found while reproducing #80).
"""
from __future__ import annotations

import logging
import threading
import time
import unittest

import nornir_pools as pools
import nornir_pools._test_pool_tasks as pool_tasks  # pyright: ignore[reportMissingImports]
import nornir_pools.multiprocessthreadpool as multiprocessthreadpool

SquareTheNumber = pool_tasks.SquareTheNumber

# A stale entry used to spin at 100% CPU forever. Anything above a second or two is
# conclusive; the pre-fix measurement in #221 ran for a full 10s budget without returning.
SPIN_BUDGET_SECONDS = 8.0


class _PoolFixture(unittest.TestCase):
    """Builds pools directly, bypassing the GetMultithreadingPool name cache."""

    def _pool(self, name, workers=2):
        pool = multiprocessthreadpool.MultiprocessThreadPool(name, workers)
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

    def _run_with_budget(self, func, budget=SPIN_BUDGET_SECONDS):
        """Run *func* on a worker thread. Returns (finished, error, elapsed)."""
        box: dict[str, BaseException | None] = {'error': None}
        finished = threading.Event()

        def runner():
            try:
                func()
            except BaseException as exc:  # noqa: BLE001 - reported to the test
                box['error'] = exc
            finally:
                finished.set()

        thread = threading.Thread(target=runner, daemon=True, name='wait_completion-probe')
        start = time.perf_counter()
        thread.start()
        completed = finished.wait(budget)
        return completed, box['error'], time.perf_counter() - start


class TestAStaleEntryDoesNotSpinForever(_PoolFixture):
    """The reproduced hot spin: a registered task whose result already arrived."""

    def _stale_entry_pool(self, name):
        pool = self._pool(name)
        task = pool.add_task('t', SquareTheNumber, 3)
        self.assertEqual(9, task.wait_return())
        # The callback removed it. Re-register to recreate the state a callback that never
        # fired would leave: present in _active_tasks, AsyncResult already complete.
        self.assertEqual(0, len(pool._active_tasks))
        pool._active_tasks[task.task_id] = task
        self.assertTrue(task.asyncresult.ready(), 'fixture premise: the result already arrived')
        return pool, task

    def test_wait_completion_returns_instead_of_spinning(self):
        pool, _ = self._stale_entry_pool('t221-stale-returns')

        finished, error, elapsed = self._run_with_budget(pool.wait_completion)

        self.assertTrue(finished,
                        f'wait_completion did not return within {SPIN_BUDGET_SECONDS}s; '
                        'it is spinning on the stale entry')
        self.assertIsNone(error)
        self.assertLess(elapsed, SPIN_BUDGET_SECONDS)

    def test_the_stale_entry_is_reaped(self):
        pool, _ = self._stale_entry_pool('t221-stale-reaped')

        finished, _, _ = self._run_with_budget(pool.wait_completion)

        self.assertTrue(finished, 'wait_completion never returned')
        self.assertEqual(0, len(pool._active_tasks),
                         'the entry the callback failed to remove must be reaped here')

    def test_it_returns_promptly_rather_than_polling_out_the_clock(self):
        """Reaping should be immediate, not a timeout expiring."""
        pool, _ = self._stale_entry_pool('t221-stale-prompt')

        finished, _, elapsed = self._run_with_budget(pool.wait_completion)

        self.assertTrue(finished)
        self.assertLess(elapsed, 2.0,
                        'a ready-but-registered entry is detectable immediately')

    def test_shutdown_no_longer_hangs_on_a_stale_entry(self):
        pool, _ = self._stale_entry_pool('t221-stale-shutdown')

        finished, error, _ = self._run_with_budget(pool.shutdown)

        self.assertTrue(finished, 'shutdown hung in wait_completion')
        self.assertIsNone(error)
        self.assertIsNone(pool._tasks)

    def test_many_stale_entries_all_drain(self):
        pool = self._pool('t221-stale-many')
        tasks = [pool.add_task(f't{i}', SquareTheNumber, i) for i in range(6)]
        for task in tasks:
            task.wait_return()
        for task in tasks:
            pool._active_tasks[task.task_id] = task

        finished, error, _ = self._run_with_budget(pool.wait_completion)

        self.assertTrue(finished, 'wait_completion spun on the stale entries')
        self.assertIsNone(error)
        self.assertEqual(0, len(pool._active_tasks))


class TestACallbackCannotKillThePool(_PoolFixture):
    """A pool callback must not raise, because it runs before AsyncResult._event.set()."""

    def test_an_unknown_task_id_is_reported_not_raised(self):
        pool = self._pool('t221-cb-unknown')
        wrapper = pool.callback_wrapper(999999, lambda result: 'called')

        with self.assertLogs('nornir_pools.poolbase', level=logging.WARNING) as logged:
            result = wrapper('ignored')

        self.assertEqual('called', result,
                         'the wrapped callback must still run so _event.set() is reached')
        self.assertTrue(any('999999' in line for line in logged.output),
                        'the unrecognised task id should be named in the log')

    def test_a_raising_inner_callback_is_contained(self):
        """Even if the task's own callback raises, nothing may escape to _handle_results."""
        pool = self._pool('t221-cb-inner-raise')

        def explode(result):
            raise RuntimeError('inner callback failure')

        task = pool.add_task('t', SquareTheNumber, 4)
        wrapper = pool.callback_wrapper(task.task_id, explode)

        with self.assertLogs('nornir_pools.poolbase', level=logging.ERROR):
            self.assertIsNone(wrapper('ignored'))

    def test_a_later_task_still_completes_after_an_unknown_id_callback(self):
        """The whole point: the pool's result handler must survive."""
        pool = self._pool('t221-cb-survives')
        pool.add_task('warm', SquareTheNumber, 1).wait_return()

        pool.callback_wrapper(888888, lambda result: None)('ignored')

        later = pool.add_task('later', SquareTheNumber, 6)
        self.assertEqual(36, later.wait_return(),
                         'a task submitted after the stray callback must still complete')

    def test_terminate_still_works_afterwards(self):
        """A dead result handler makes pool.terminate() raise, disabling terminate_workers."""
        pool = self._pool('t221-cb-terminate')
        pool.add_task('warm', SquareTheNumber, 1).wait_return()

        pool.callback_wrapper(777777, lambda result: None)('ignored')

        pool.terminate_workers()  # must not raise
        self.assertIsNone(pool._tasks)

    def test_a_normal_callback_still_removes_its_task(self):
        """The fix must not break the ordinary path."""
        pool = self._pool('t221-cb-normal')
        task = pool.add_task('t', SquareTheNumber, 5)
        self.assertEqual(25, task.wait_return())

        pool.wait_completion()

        self.assertEqual(0, len(pool._active_tasks))


class TestTheBoundedEscape(_PoolFixture):
    """timeout= is the escape for a task whose AsyncResult never completes at all."""

    class _NeverReady:
        """Stands in for the swallowed-import-error task add_task's comment describes."""

        def __init__(self):
            self._event = threading.Event()

        def wait(self, timeout=None):
            self._event.wait(timeout)

        def ready(self):
            return False

        def successful(self):
            return False

    def _wedged_pool(self, name):
        pool = self._pool(name)
        task = multiprocessthreadpool.MultiprocessThreadTask('wedged', self._NeverReady())
        pool._active_tasks[task.task_id] = task
        return pool, task

    def test_a_never_ready_task_times_out(self):
        pool, task = self._wedged_pool('t221-timeout')

        finished, error, elapsed = self._run_with_budget(
            lambda: pool.wait_completion(timeout=1.0))

        self.assertTrue(finished, 'the bounded wait did not return')
        self.assertIsInstance(error, TimeoutError)
        self.assertGreaterEqual(elapsed, 0.9, 'it should actually wait for the budget')

    def test_the_timeout_names_the_stuck_task(self):
        pool, task = self._wedged_pool('t221-timeout-names')

        _, error, _ = self._run_with_budget(lambda: pool.wait_completion(timeout=0.5))

        self.assertIsInstance(error, TimeoutError)
        self.assertIn(str(task.task_id), str(error),
                      'the stuck task id must be identifiable from the error')

    def test_an_unbounded_wait_on_a_never_ready_task_does_not_burn_cpu(self):
        """Without a timeout it still hangs -- but as a block, not a spin."""
        pool, _ = self._wedged_pool('t221-unbounded-no-spin')

        finished, _, _ = self._run_with_budget(pool.wait_completion, budget=2.0)

        self.assertFalse(finished, 'premise: an unbounded wait on a never-ready task blocks')
        # If it were spinning, this thread would have re-entered the loop thousands of
        # times. The task is only asked to wait once per iteration, and each wait blocks.
        self.assertEqual(1, len(pool._active_tasks))

    def test_a_timeout_does_not_fire_when_tasks_finish_in_time(self):
        pool = self._pool('t221-timeout-not-hit')
        for i in range(4):
            pool.add_task(f't{i}', SquareTheNumber, i)

        pool.wait_completion(timeout=120.0)

        self.assertEqual(0, len(pool._active_tasks))

    def test_shutdown_is_unbounded_by_default(self):
        """A finite default would eventually kill legitimate long-running work."""
        self.assertIsNone(multiprocessthreadpool._shutdown_timeout())

    def test_shutdown_timeout_is_configurable(self):
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, {'NORNIR_POOL_SHUTDOWN_TIMEOUT': '12.5'}):
            self.assertEqual(12.5, multiprocessthreadpool._shutdown_timeout())

    def test_a_bad_shutdown_timeout_falls_back_to_unbounded(self):
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, {'NORNIR_POOL_SHUTDOWN_TIMEOUT': 'soon'}):
            self.assertIsNone(multiprocessthreadpool._shutdown_timeout())
        with mock.patch.dict(os.environ, {'NORNIR_POOL_SHUTDOWN_TIMEOUT': '-5'}):
            self.assertIsNone(multiprocessthreadpool._shutdown_timeout())

    def test_shutdown_terminates_workers_when_its_budget_expires(self):
        import os
        from unittest import mock

        pool, _ = self._wedged_pool('t221-shutdown-budget')

        with mock.patch.dict(os.environ, {'NORNIR_POOL_SHUTDOWN_TIMEOUT': '1'}):
            finished, error, _ = self._run_with_budget(pool.shutdown)

        self.assertTrue(finished, 'shutdown did not return within its budget')
        self.assertIsNone(error, 'teardown must not raise')
        self.assertIsNone(pool._tasks)
        self.assertEqual(0, len(pool._active_tasks))


class TestExistingSemanticsArePreserved(_PoolFixture):
    """wait_completion still re-raises worker exceptions and still drains normally."""

    def test_worker_exceptions_still_propagate(self):
        pool = self._pool('t221-exceptions')
        pool.add_task('boom', pool_tasks.RaiseException, 'expected failure')

        with self.assertRaises(Exception):
            pool.wait_completion()

    def test_a_normal_batch_drains(self):
        pool = self._pool('t221-normal-batch')
        tasks = [pool.add_task(f't{i}', SquareTheNumber, i) for i in range(8)]

        pool.wait_completion()

        self.assertEqual(0, len(pool._active_tasks))
        for i, task in enumerate(tasks):
            self.assertEqual(i * i, task.wait_return())

    def test_wait_completion_still_takes_no_arguments(self):
        """~20 call sites across the monorepo call this with no arguments."""
        pool = self._pool('t221-no-args')
        pool.add_task('t', SquareTheNumber, 2)

        pool.wait_completion()

        self.assertEqual(0, len(pool._active_tasks))


if __name__ == '__main__':
    unittest.main()
