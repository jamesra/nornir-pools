"""A worker waiting on a task queued behind it on the same pool starves the pool.

With ``num_threads=N``, submit N tasks that each submit a child to the *same* pool and
block on it. Each parent occupies a worker while its child sits in the queue behind it.
No worker is left to run any child, so nothing completes and ``wait_completion`` never
returns. #85 fixed the queue-capacity half of this -- a worker submitting onto its own
pool is no longer throttled -- but submitting is only half the pattern, so this survived
that fix.

The codebase guards the invariant with a docstring rather than a mechanism::

    nornir-imageregistration/.../meshwithrbffallback.py
    "Call this from a non-pool driver thread ... so workers are not nested-waiting on
     the same pool."

and the two nesting chains that exist today are safe only because they happen to cross
pool identities. ``GetGlobalThreadPool()`` is a process-wide singleton, so any helper
that calls it while already running on it closes the loop.

**What this change does and does not do.** It does not prevent the deadlock. Every fix
that would is a material behavioural change: work-stealing means re-entrant task
execution on a thread that is mid-task, which can surprise anything holding a lock, and
growing the pool breaks the ``num_threads`` contract that ``add_threads_if_needed``
accounts against. So this makes the fragility *visible* -- a throttled warning while a
nested wait is merely wasteful, and an unthrottled error once enough workers are parked
that the pool provably cannot drain.

That required recording the owning pool on each task, which is what makes the condition
detectable at all. Detection reuses ``called_from_pool_worker()`` from #85, so it is exact
rather than inferred from thread names.

Review issue #222 (found while fixing #85).
"""
from __future__ import annotations

import logging
import threading
import time
import unittest

import nornir_pools as pools
import nornir_pools._test_pool_tasks as pool_tasks  # pyright: ignore[reportMissingImports]
import nornir_pools.poolbase as poolbase
import nornir_pools.threadpool as threadpool

SquareTheNumber = pool_tasks.SquareTheNumber

POOL_LOGGER = 'nornir_pools.poolbase'

# The deadlock is permanent, so any budget proves it. Kept short since the test asserts
# the wedge rather than waiting it out.
DEADLOCK_BUDGET_SECONDS = 4.0


class _ThreadPoolFixture(unittest.TestCase):

    def _pool(self, name, workers=2):
        pool = threadpool.ThreadPool(name, workers)
        self.addCleanup(self._force_cleanup, pool)
        return pool

    @staticmethod
    def _force_cleanup(pool):
        try:
            pool.shutdown_event.set()
        except Exception:
            pass
        try:
            pools._remove_pool(pool)
        except Exception:
            pass


class TestTheTaskKnowsItsPool(_ThreadPoolFixture):
    """Recording the pool is what makes detection possible."""

    def test_a_thread_pool_task_records_its_pool(self):
        pool = self._pool('t222-records')
        task = pool.add_task('t', SquareTheNumber, 3)
        self.assertIs(pool, task.pool)
        task.wait_return()

    def test_the_reference_is_weak(self):
        """A stray task object must not keep a pool, and its threads, alive.

        Asserted against a stand-in rather than a real pool: live ``Worker`` threads hold
        their pool strongly, so a real pool stays reachable until those threads exit and
        the test could not tell the task apart from them.
        """
        import gc
        import weakref

        class _PoolStandIn:
            pass

        stand_in = _PoolStandIn()
        observer = weakref.ref(stand_in)
        task = threadpool.ThreadTask('t', SquareTheNumber, 1)
        task.pool = stand_in

        self.assertIs(stand_in, task.pool)

        del stand_in
        gc.collect()

        self.assertIsNone(observer(), 'the task is holding its pool alive')
        self.assertIsNone(task.pool, 'pool should read as None once collected')

    def test_a_task_without_a_pool_reads_none(self):
        task = threadpool.ThreadTask('orphan', SquareTheNumber, 2)
        self.assertIsNone(task.pool)

    def test_waiting_on_a_poolless_task_still_works(self):
        """The guard must not be required for a task nobody claimed."""
        task = threadpool.ThreadTask('orphan', SquareTheNumber, 2)
        task.returned_value = 4
        task.completed.set()

        self.assertEqual(4, task.wait_return())


class TestANestedWaitIsReported(_ThreadPoolFixture):
    """One worker waiting on its own pool is wasteful but not yet fatal."""

    def test_a_nested_wait_warns(self):
        pool = self._pool('t222-warn', workers=4)
        seen: dict[str, object] = {}

        def parent():
            child = pool.add_task('child', SquareTheNumber, 7)
            seen['child_result'] = child.wait_return()

        with self.assertLogs(POOL_LOGGER, level=logging.WARNING) as captured:
            pool.add_task('parent', parent).wait_return()

        self.assertEqual(49, seen['child_result'],
                         'with spare workers the nested wait still completes')
        self.assertTrue(any('own pool' in line for line in captured.output),
                        f'expected a nested-wait warning, got {captured.output}')

    def test_the_warning_names_the_pool_and_task(self):
        pool = self._pool('t222-warn-names', workers=4)

        def parent():
            pool.add_task('the-child-task', SquareTheNumber, 7).wait_return()

        with self.assertLogs(POOL_LOGGER, level=logging.WARNING) as captured:
            pool.add_task('parent', parent).wait_return()

        joined = '\n'.join(captured.output)
        self.assertIn('t222-warn-names', joined)
        self.assertIn('the-child-task', joined)

    def test_an_ordinary_outside_wait_is_silent(self):
        """The overwhelmingly common case must not log anything."""
        pool = self._pool('t222-quiet', workers=2)

        with self.assertNoLogs(POOL_LOGGER, level=logging.WARNING):
            for i in range(6):
                pool.add_task(f't{i}', SquareTheNumber, i).wait_return()

    def test_the_warning_is_throttled(self):
        """A pool used this way throughout a run must not flood the log."""
        pool = self._pool('t222-throttle', workers=4)

        def parent():
            pool.add_task('child', SquareTheNumber, 1).wait_return()

        with self.assertLogs(POOL_LOGGER, level=logging.WARNING) as captured:
            for _ in range(5):
                pool.add_task('parent', parent).wait_return()

        nested = [line for line in captured.output if 'own pool' in line]
        self.assertEqual(1, len(nested),
                         f'expected one throttled warning, got {len(nested)}: {nested}')

    def test_the_counter_unwinds(self):
        pool = self._pool('t222-unwind', workers=4)

        def parent():
            pool.add_task('child', SquareTheNumber, 1).wait_return()

        pool.add_task('parent', parent).wait_return()

        self.assertEqual(0, pool._nested_waiters,
                         'the nested-wait count must return to zero')

    def test_the_counter_unwinds_when_the_child_raises(self):
        pool = self._pool('t222-unwind-raise', workers=4)

        def parent():
            try:
                pool.add_task('child', pool_tasks.RaiseException, 'expected').wait_return()
            except Exception:
                pass

        pool.add_task('parent', parent).wait_return()

        self.assertEqual(0, pool._nested_waiters)


class TestTheDeadlockIsReported(_ThreadPoolFixture):
    """Every worker parked on its own pool with work queued: no longer a risk, a fact."""

    def _wedge(self, pool, workers):
        """Occupy every worker with a parent, then have each block on a child of this pool.

        Two details are load-bearing, both measured rather than assumed.

        No child is submitted until every worker is already occupied.
        ``add_threads_if_needed`` tops the pool up towards ``num_threads`` whenever a task
        is queued, so a parent that submits its child early lets the pool spawn a spare
        worker which then runs the child, and nothing deadlocks.

        More parents are submitted than there are workers. That same top-up loop stops
        early when a worker empties the queue mid-loop, so submitting exactly ``workers``
        tasks can leave the pool a thread short -- measured 3 threads for a
        ``ThreadPool(4)`` with 4 tasks queued, with the fourth parent still in the queue.
        The surplus parents guarantee every worker thread exists and is busy; the ones
        that never get a thread simply sit in the queue.

        Returns the list of child tasks, which stay incomplete for as long as the pool is
        wedged.
        """
        hold = threading.Event()
        children: list = []
        children_lock = threading.Lock()

        def parent():
            if not hold.wait(DEADLOCK_BUDGET_SECONDS * 3):
                return
            child = pool.add_task('child', SquareTheNumber, 1)
            with children_lock:
                children.append(child)
            child.wait_return()

        for i in range(workers * 2):
            pool.add_task(f'parent{i}', parent)

        deadline = time.monotonic() + DEADLOCK_BUDGET_SECONDS
        while pool.active_tasks < workers and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertGreaterEqual(
            pool.active_tasks, workers,
            'fixture premise: every worker should be occupied by a parent before any '
            'child is submitted')

        hold.set()
        return children

    def test_the_deadlock_is_logged_as_an_error(self):
        workers = 2
        pool = self._pool('t222-deadlock', workers=workers)

        with self.assertLogs(POOL_LOGGER, level=logging.ERROR) as captured:
            self._wedge(pool, workers)
            time.sleep(DEADLOCK_BUDGET_SECONDS)

        self.assertTrue(any('deadlocked' in line for line in captured.output),
                        f'expected a deadlock report, got {captured.output}')

    def test_the_deadlock_report_names_the_pool(self):
        workers = 2
        pool = self._pool('t222-deadlock-named', workers=workers)

        with self.assertLogs(POOL_LOGGER, level=logging.ERROR) as captured:
            self._wedge(pool, workers)
            time.sleep(DEADLOCK_BUDGET_SECONDS)

        self.assertIn('t222-deadlock-named', '\n'.join(captured.output))

    def test_the_pool_really_is_wedged(self):
        """Confirms the premise: this is a real deadlock, not just a noisy warning."""
        workers = 2
        pool = self._pool('t222-really-wedged', workers=workers)
        children = self._wedge(pool, workers)

        drained = threading.Event()

        def drain():
            pool.wait_completion()
            drained.set()

        threading.Thread(target=drain, daemon=True).start()

        self.assertFalse(drained.wait(DEADLOCK_BUDGET_SECONDS),
                         'premise failed: the pool drained, so this is not a deadlock')
        self.assertTrue(children, 'no child was ever submitted')
        self.assertFalse(any(child.iscompleted for child in children),
                         'a queued child ran, so the workers were not all starved')

    def test_the_deadlock_error_is_not_throttled(self):
        """A warning suppressed by the interval must not hide the deadlock behind it."""
        workers = 2
        pool = self._pool('t222-deadlock-unthrottled', workers=workers)
        # Pretend a nested-wait warning just fired, which would silence a warning.
        pool._last_nested_wait_warning = time.monotonic()

        with self.assertLogs(POOL_LOGGER, level=logging.ERROR) as captured:
            self._wedge(pool, workers)
            time.sleep(DEADLOCK_BUDGET_SECONDS)

        self.assertTrue(any('deadlocked' in line for line in captured.output))


class TestDetectionIsExact(_ThreadPoolFixture):
    """It must not fire for a different pool, which is what today's real chains do."""

    def test_waiting_across_two_pools_is_not_reported(self):
        outer = self._pool('t222-outer', workers=2)
        inner = self._pool('t222-inner', workers=2)
        seen = {}

        def parent():
            seen['result'] = inner.add_task('child', SquareTheNumber, 9).wait_return()

        with self.assertNoLogs(POOL_LOGGER, level=logging.WARNING):
            outer.add_task('parent', parent).wait_return()

        self.assertEqual(81, seen['result'])

    def test_the_guard_is_a_noop_off_pool(self):
        pool = self._pool('t222-offpool', workers=2)

        with pool.nested_wait_guard('anything'):
            self.assertEqual(0, pool._nested_waiters,
                             'a caller that is not a worker must not be counted')

    def test_called_from_pool_worker_distinguishes_pools(self):
        outer = self._pool('t222-distinguish-outer', workers=1)
        inner = self._pool('t222-distinguish-inner', workers=1)
        answers = {}

        def probe():
            answers['own'] = outer.called_from_pool_worker()
            answers['other'] = inner.called_from_pool_worker()

        outer.add_task('probe', probe).wait_return()

        self.assertTrue(answers['own'])
        self.assertFalse(answers['other'])


if __name__ == '__main__':
    unittest.main()
