"""Pool workers cap their BLAS/OpenMP threads at ceil(cores / workers).

Without a cap every worker sized its BLAS pool to the whole machine: on srv's 128
cores each of ~128 pool workers started 127 BLAS threads, and pools doing linear
algebra at once (RBF fallbacks during overlays on RC2, 2026-11-02) ran each worker at
300-600% CPU, oversubscribing the cores.
"""

from __future__ import annotations

import multiprocessing
import os
import unittest
from unittest import mock

import nornir_pools
from nornir_pools import _test_pool_tasks


class TestBlasThreadsPerWorker(unittest.TestCase):

    def test_divides_cores_by_workers_rounding_up(self):
        with mock.patch.object(os, 'cpu_count', return_value=128), \
                mock.patch.dict(os.environ, {nornir_pools.BLAS_THREADS_ENV: ''}):
            self.assertEqual(nornir_pools.blas_threads_per_worker(128), 1)
            self.assertEqual(nornir_pools.blas_threads_per_worker(14), 10)
            self.assertEqual(nornir_pools.blas_threads_per_worker(200), 1)
            self.assertEqual(nornir_pools.blas_threads_per_worker(None), 1)

    def test_environment_overrides(self):
        with mock.patch.dict(os.environ, {nornir_pools.BLAS_THREADS_ENV: '3'}):
            self.assertEqual(nornir_pools.blas_threads_per_worker(128), 3)

    def test_none_is_a_no_op(self):
        before = {name: os.environ.get(name) for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS')}
        nornir_pools.limit_blas_threads(None)
        self.assertEqual(before, {name: os.environ.get(name) for name in before})


class TestPoolWorkersAreCapped(unittest.TestCase):
    """Workers report the cap they were given (1 here, so it is visible on any machine)."""

    def setUp(self):
        try:
            import threadpoolctl  # noqa: F401
        except ImportError:
            self.skipTest('threadpoolctl not installed')

    def test_multiprocess_pool_workers_run_with_the_cap(self):
        pool = nornir_pools.GetMultithreadingPool('blas-limit-test', num_threads=2, blas_threads=1)
        try:
            tasks = [pool.add_task(str(i), _test_pool_tasks.ReportBlasThreads, i) for i in range(4)]
            seen = {task.wait_return() for task in tasks}
        finally:
            pool.shutdown()

        self.assertEqual(seen, {1})

    def test_process_pool_executor_workers_run_with_the_cap(self):
        context = multiprocessing.get_context('spawn')
        with nornir_pools.CreateProcessPoolExecutor(2, mp_context=context, blas_threads=1) as executor:
            seen = set(executor.map(_test_pool_tasks.ReportBlasThreads, range(4)))

        self.assertEqual(seen, {1})

    def test_executor_runs_the_callers_initializer_too(self):
        context = multiprocessing.get_context('spawn')
        with nornir_pools.CreateProcessPoolExecutor(1, mp_context=context, initializer=_test_pool_tasks.SetEnvironment,
                                                    initargs=('NORNIR_TEST_INIT_RAN', '1')) as executor:
            ran = executor.submit(_test_pool_tasks.GetEnvironment, 'NORNIR_TEST_INIT_RAN').result()

        self.assertEqual(ran, '1')


if __name__ == '__main__':
    unittest.main()
