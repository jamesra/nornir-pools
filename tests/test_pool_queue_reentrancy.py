"""A pool worker must never be throttled by the queue it is draining.

``LocalThreadPoolBase`` gave every pool a queue bounded at ``num_threads * 32`` and both
``ThreadPool.add_task`` and ``ProcessPool.add_process`` put onto it with a blocking,
untimed ``put``. When the producer was one of the pool's own workers, that put blocked
the thread that had to drain the queue for the put to succeed. Once every worker was
blocked that way nothing could ever drain it, and ``wait_completion`` -> ``tasks.join()``
never returned.

Reproduced before the fix with two workers and a queue of 64, in a child process because
the state is not recoverable::

    workers=2 queue_maxsize=64
    all workers occupied
    queue filled: qsize=64 of 64
    RESULT submits_returned=0 of 2
    RESULT verdict=DEADLOCK every worker is blocked in queue.put
    RESULT qsize_still=64

The queue is now unbounded and the capacity is applied in ``enqueue_task`` only to
producers that are not pool workers, so outside callers still get backpressure.

Every wait here is bounded. A regression must fail these tests rather than hang the
suite, so nothing calls ``tasks.join()`` or an untimed ``wait()`` on a path that a
regression would wedge.
"""

from __future__ import annotations

import threading
import time
import unittest

import nornir_pools
from nornir_pools import poolbase, processpool, threadpool

# Generous, but bounded: a regression must fail rather than hang.
TIMEOUT = 30.0


class _PoolFixture(unittest.TestCase):
    def setUp(self):
        self._release = threading.Event()
        # Always let held workers go, even if an assertion fails part way through.
        self.addCleanup(self._release.set)

    def _pool(self, num_workers: int = 2, name: str | None = None) -> threadpool.ThreadPool:
        """A pool built directly, so the process-wide pool cache is untouched."""
        pool = threadpool.ThreadPool(name=name or self.id(), num_workers=num_workers)
        self.addCleanup(self._retire, pool)
        return pool

    def _retire(self, pool):
        self._release.set()
        pool.shutdown_event.set()
        # Tolerate a pool without the room condition, so the checks that are meant to
        # pass against the previous code are not masked by a fixture AttributeError.
        room = getattr(pool, '_queue_room', None)
        if room is not None:
            with room:
                room.notify_all()

    @staticmethod
    def _capacity(pool) -> int:
        """Queue capacity however this version of the pool expresses it."""
        return getattr(pool, '_queue_capacity', None) or pool.tasks.maxsize

    def _hold_every_worker(self, pool, num_workers: int):
        """Occupy every worker with a task that blocks until the fixture releases it."""
        started = threading.Barrier(num_workers + 1, timeout=TIMEOUT)

        def hold():
            started.wait()
            self._release.wait(TIMEOUT)

        for i in range(num_workers):
            pool.add_task(f'hold{i}', hold)
        started.wait()

    def _fill_to_capacity(self, pool):
        """Fill the queue without going through add_task, so this cannot block."""
        while pool.tasks.qsize() < self._capacity(pool):
            pool.tasks.put_nowait(
                threadpool.ThreadTask(f'pad{pool.tasks.qsize()}', lambda: 1))


class TestAWorkerIsNeverThrottled(_PoolFixture):
    """The deadlock itself."""

    def test_a_worker_can_submit_onto_a_full_queue(self):
        num_workers = 2
        pool = self._pool(num_workers)

        at_barrier = threading.Barrier(num_workers + 1, timeout=TIMEOUT)
        queue_is_full = threading.Event()
        returned: list[int] = []
        lock = threading.Lock()

        def body(index):
            at_barrier.wait()
            # Only submit once the queue is known to be over capacity, so the test
            # cannot pass by racing ahead of the fill below.
            queue_is_full.wait(TIMEOUT)
            pool.add_task(f'child{index}', lambda: 1)
            with lock:
                returned.append(index)

        for i in range(num_workers):
            pool.add_task(f'body{i}', lambda i=i: body(i))
        at_barrier.wait()

        self._fill_to_capacity(pool)
        self.assertGreaterEqual(pool.tasks.qsize(), self._capacity(pool))
        queue_is_full.set()

        deadline = time.time() + TIMEOUT
        while time.time() < deadline:
            with lock:
                if len(returned) == num_workers:
                    break
            time.sleep(0.02)

        with lock:
            got = len(returned)
        self.assertEqual(
            got, num_workers,
            f'{num_workers - got} of {num_workers} workers were still blocked in '
            f'add_task on a full queue; that is the deadlock')

    def test_a_worker_submitting_repeatedly_past_capacity_does_not_block(self):
        pool = self._pool(1)
        capacity = self._capacity(pool)
        over_capacity = capacity + 25
        done = threading.Event()
        submitted = []

        def body():
            for i in range(over_capacity):
                pool.add_task(f'child{i}', lambda: 1)
                submitted.append(i)
            done.set()

        pool.add_task('body', body)

        self.assertTrue(
            done.wait(TIMEOUT),
            f'a worker stalled after {len(submitted)} of {over_capacity} submits '
            f'against a capacity of {capacity}')

    def test_called_from_pool_worker_is_true_only_inside_that_pool(self):
        pool = self._pool(1)
        other = self._pool(1, name='other')
        seen = {}

        def body():
            seen['own'] = pool.called_from_pool_worker()
            seen['other'] = other.called_from_pool_worker()

        pool.add_task('body', body).wait()

        self.assertFalse(pool.called_from_pool_worker(),
                         'the calling thread is not a worker')
        self.assertTrue(seen['own'], 'a worker must recognise its own pool')
        self.assertFalse(seen['other'],
                         'a worker of one pool is an outside producer to another')


class TestOutsideProducersStillGetBackpressure(_PoolFixture):
    """The bound has a purpose; the fix must not simply remove it."""

    def test_a_full_queue_makes_an_outside_producer_wait(self):
        num_workers = 2
        pool = self._pool(num_workers)
        self._hold_every_worker(pool, num_workers)
        self._fill_to_capacity(pool)

        entered = threading.Event()
        proceeded = threading.Event()

        def producer():
            entered.set()
            pool.add_task('over_capacity', lambda: 1)
            proceeded.set()

        threading.Thread(target=producer, daemon=True).start()
        self.assertTrue(entered.wait(TIMEOUT))

        self.assertFalse(
            proceeded.wait(2.0),
            'an outside producer sailed past a full queue; backpressure is gone')

        # It must be a wait, not a hang: draining the queue releases it.
        self._release.set()
        self.assertTrue(proceeded.wait(TIMEOUT),
                        'the producer was never released after the queue drained')

    def test_the_capacity_is_still_per_worker(self):
        for num_workers in (1, 2, 4):
            with self.subTest(num_workers=num_workers):
                pool = self._pool(num_workers, name=f'cap{num_workers}')
                expected = ((pool._max_threads or 1)
                            * poolbase._QUEUE_CAPACITY_PER_WORKER)
                self.assertEqual(pool._queue_capacity, expected)

    def test_shutdown_releases_a_waiting_producer(self):
        num_workers = 2
        pool = self._pool(num_workers)
        self._hold_every_worker(pool, num_workers)
        self._fill_to_capacity(pool)

        proceeded = threading.Event()

        def producer():
            pool._wait_for_queue_room()
            proceeded.set()

        threading.Thread(target=producer, daemon=True).start()
        self.assertFalse(proceeded.wait(1.0), 'should be waiting for room')

        pool.shutdown_event.set()
        with pool._queue_room:
            pool._queue_room.notify_all()

        self.assertTrue(proceeded.wait(TIMEOUT),
                        'a producer waiting for room must not survive shutdown')


class TestTheQueueIsNoLongerBounded(_PoolFixture):
    """Pin the mechanism, so a future edit cannot quietly restore the maxsize."""

    def test_the_task_queue_has_no_maxsize(self):
        pool = self._pool(2)

        self.assertEqual(
            pool.tasks.maxsize, 0,
            'a maxsize on the queue means a blocking put can wedge a worker again')

    def test_capacity_is_recorded_separately_from_the_queue(self):
        pool = self._pool(2)

        self.assertGreater(pool._queue_capacity, 0)
        self.assertNotEqual(pool._queue_capacity, pool.tasks.maxsize)

    def test_both_local_thread_pools_enqueue_through_the_guard(self):
        """ThreadPool and ProcessPool share the queue, so both must share the fix."""
        import inspect

        for pool_cls, method in ((threadpool.ThreadPool, 'add_task'),
                                 (processpool.ProcessPool, 'add_process')):
            with self.subTest(pool=pool_cls.__name__):
                source = inspect.getsource(getattr(pool_cls, method))
                self.assertIn('enqueue_task', source,
                              f'{pool_cls.__name__}.{method} must not put directly')
                self.assertNotIn('self.tasks.put', source)

    def test_the_process_pool_queue_is_also_unbounded(self):
        pool = processpool.ProcessPool(name='proc_unbounded', num_workers=2)
        self.addCleanup(self._retire, pool)

        self.assertEqual(pool.tasks.maxsize, 0)
        self.assertGreater(pool._queue_capacity, 0)


class TestOrdinaryUseIsUnaffected(_PoolFixture):
    """Throughput, results, exceptions and completion must all behave as before."""

    def test_many_tasks_return_their_values(self):
        pool = self._pool(4)
        count = 300

        tasks = [pool.add_task(f't{i}', lambda i=i: i * 3) for i in range(count)]
        values = [t.wait_return() for t in tasks]

        self.assertEqual(values, [i * 3 for i in range(count)])

    def test_a_batch_larger_than_the_capacity_completes(self):
        pool = self._pool(2)
        count = self._capacity(pool) * 2

        tasks = [pool.add_task(f't{i}', lambda i=i: i) for i in range(count)]
        values = [t.wait_return() for t in tasks]

        self.assertEqual(values, list(range(count)))

    def test_wait_completion_returns_when_the_queue_drains(self):
        pool = self._pool(4)
        for i in range(100):
            pool.add_task(f't{i}', lambda: 1)

        finished = threading.Event()

        def joiner():
            pool.wait_completion()
            finished.set()

        threading.Thread(target=joiner, daemon=True).start()

        self.assertTrue(finished.wait(TIMEOUT), 'wait_completion did not return')
        self.assertEqual(pool.tasks.qsize(), 0)

    def test_an_exception_still_reaches_the_caller(self):
        pool = self._pool(2)

        def boom():
            raise ValueError('expected')

        entry = pool.add_task('boom', boom)

        with self.assertRaises(ValueError) as caught:
            entry.wait_return()
        self.assertEqual(str(caught.exception), 'expected')

    def test_queued_and_active_counts_still_report(self):
        num_workers = 2
        pool = self._pool(num_workers)
        self._hold_every_worker(pool, num_workers)
        for i in range(5):
            pool.add_task(f'queued{i}', lambda: 1)

        self.assertGreaterEqual(pool.queued_tasks, 1)
        self.assertGreaterEqual(pool.active_tasks, 1)
        self.assertGreaterEqual(pool.num_active_tasks, pool.queued_tasks)

    def test_the_documented_factory_path_still_works(self):
        pool = nornir_pools.GetThreadPool('test_pool_queue_reentrancy_factory',
                                          num_threads=2)
        self.addCleanup(self._retire, pool)

        entry = pool.add_task('t', lambda: 'value')

        self.assertEqual(entry.wait_return(), 'value')


if __name__ == '__main__':
    unittest.main()
