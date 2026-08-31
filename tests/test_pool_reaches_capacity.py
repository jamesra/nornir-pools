"""A pool asked for N threads and given N tasks must be able to run all N (review #233).

`add_threads_if_needed` used to size the pool from `qsize() + 1` and then re-check
`not self.tasks.empty()` before each creation. A worker dequeueing the last item between the
two cancelled a thread the pool had already decided it needed, and nothing revisited that
decision until the next `add_task` -- which, for the final task of a batch, never comes. The
result was deterministic, not flaky: `ThreadPool(4)` given 4 tasks settled at 3 threads with
one task queued, so short batches took twice as long as they should.

The barrier tests below are the direct statement of the defect: a barrier of N parties can
only trip if N tasks are genuinely running at the same time, so it times out on the old code
and passes on the new. The remaining tests pin the two ways a fix could overshoot -- creating
threads a pool does not need, or exceeding the `num_threads` contract.
"""

from __future__ import annotations

import threading
import time

import pytest

from nornir_pools.threadpool import ThreadPool

_TRIP_TIMEOUT = 20.0


@pytest.fixture
def pool_factory():
    """Hand out pools and guarantee shutdown even when an assertion fails mid-batch."""
    pools = []

    def make(num_workers: int, name: str) -> ThreadPool:
        p = ThreadPool(num_workers=num_workers, name=name)
        pools.append(p)
        return p

    yield make

    for p in pools:
        try:
            p.shutdown()
        except Exception:  # noqa: BLE001 - teardown must not mask the real failure
            pass


def _run_concurrent_batch(pool, n: int, timeout: float = _TRIP_TIMEOUT):
    """Submit n tasks that each wait on an n-party barrier; report whether all n ran.

    Returns (tripped, tasks). The barrier can only trip if the pool runs all n at once, so
    this measures concurrency rather than inferring it from a thread count.
    """
    barrier = threading.Barrier(n)
    tripped = threading.Event()

    def body():
        try:
            barrier.wait(timeout)
        except threading.BrokenBarrierError:
            return
        tripped.set()

    tasks = [pool.add_task(f't{i}', body) for i in range(n)]
    # Give the pool the same chance a real producer would: the old code's shortfall appeared
    # only after the final submission, so there is nothing left to trigger a top-up.
    tripped.wait(timeout)
    barrier.abort()
    for t in tasks:
        try:
            t.wait()
        except Exception:  # noqa: BLE001 - a broken barrier is not what we are asserting
            pass
    return tripped.is_set(), tasks


class TestAllTheRequestedWorkersRun:

    @pytest.mark.parametrize('n', [2, 4, 8])
    def test_n_tasks_on_a_pool_of_n_all_run_at_once(self, pool_factory, n):
        pool = pool_factory(n, f'cap-{n}')
        tripped, _ = _run_concurrent_batch(pool, n)
        assert tripped, (
            f'ThreadPool({n}) could not run {n} tasks concurrently; '
            f'threads={len(pool._threads)} queued={pool.queued_tasks}')

    def test_it_is_deterministic_not_flaky(self, pool_factory):
        """The old shortfall reproduced 5 of 5 trials, so one pass is not evidence."""
        for trial in range(5):
            pool = pool_factory(4, f'cap-repeat-{trial}')
            tripped, _ = _run_concurrent_batch(pool, 4)
            assert tripped, f'trial {trial} failed to reach capacity'

    def test_no_task_is_left_queued_after_the_batch_is_submitted(self, pool_factory):
        n = 4
        pool = pool_factory(n, 'cap-noqueue')
        release = threading.Event()
        tasks = [pool.add_task(f't{i}', lambda: release.wait(_TRIP_TIMEOUT))
                 for i in range(n)]
        deadline = time.monotonic() + _TRIP_TIMEOUT
        while time.monotonic() < deadline and pool.active_tasks < n:
            time.sleep(0.01)

        assert pool.queued_tasks == 0, (
            f'{pool.queued_tasks} task(s) still queued with only '
            f'{len(pool._threads)} of {n} threads created')
        assert pool.active_tasks == n

        release.set()
        for t in tasks:
            t.wait()


class TestItDoesNotOvershoot:
    """The fix sizes against queued+inflight, so guard both directions of overshoot."""

    def test_the_thread_count_never_exceeds_num_threads(self, pool_factory):
        n = 4
        pool = pool_factory(n, 'cap-ceiling')
        release = threading.Event()
        tasks = [pool.add_task(f't{i}', lambda: release.wait(_TRIP_TIMEOUT))
                 for i in range(n * 25)]
        assert len(pool._threads) <= n, (
            f'{len(pool._threads)} threads on a pool of {n}')
        release.set()
        for t in tasks:
            t.wait()
        assert len(pool._threads) <= n

    def test_one_task_does_not_spin_up_the_whole_pool(self, pool_factory):
        pool = pool_factory(8, 'cap-single')
        task = pool.add_task('only', lambda: None)
        assert len(pool._threads) <= 1, (
            f'a single task created {len(pool._threads)} threads')
        task.wait()

    def test_idle_workers_are_reused_rather_than_added_to(self, pool_factory):
        """N idle threads plus one task needs no new thread: the target is 1, not N+1.

        This is the case that rules out the naive `threads + qsize` target, which would
        assume every existing thread is busy and grow a pool that is entirely idle.
        """
        n = 4
        pool = pool_factory(n, 'cap-idle')
        tripped, tasks = _run_concurrent_batch(pool, n)
        assert tripped
        for t in tasks:
            t.wait()

        deadline = time.monotonic() + _TRIP_TIMEOUT
        while time.monotonic() < deadline and pool.active_tasks > 0:
            time.sleep(0.01)
        idle_threads = len(pool._threads)
        assert idle_threads > 0, 'expected the drained pool to retain its workers'

        follow_up = pool.add_task('after', lambda: None)
        assert len(pool._threads) == idle_threads, (
            f'one task on a pool with {idle_threads} idle threads created '
            f'{len(pool._threads) - idle_threads} more')
        follow_up.wait()


class TestTheSumIsWhatMakesItSafe:
    """queued + inflight is conserved by a dequeue; that is why the target cannot be lost."""

    def test_a_dequeue_moves_work_between_the_terms_without_changing_the_sum(
            self, pool_factory):
        n = 4
        pool = pool_factory(n, 'cap-invariant')
        release = threading.Event()
        tasks = []
        for i in range(n):
            tasks.append(pool.add_task(f't{i}', lambda: release.wait(_TRIP_TIMEOUT)))
            # Sampling the two terms is not atomic, so allow the sum to read one high --
            # the direction the fix deliberately errs in. It must never read low, which is
            # what would let a needed thread go uncreated.
            total = pool.queued_tasks + pool.active_tasks
            assert total >= i + 1 - 1, (
                f'after {i + 1} submissions the outstanding count read {total}')

        deadline = time.monotonic() + _TRIP_TIMEOUT
        while time.monotonic() < deadline and pool.active_tasks < n:
            time.sleep(0.01)
        assert pool.queued_tasks + pool.active_tasks == n

        release.set()
        for t in tasks:
            t.wait()
