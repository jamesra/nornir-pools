"""A missing Parallel Python backend must say so, not raise NameError.

``ParallelPythonProcess_Pool.server`` dereferenced a module-level ``pp`` that the module
never imported. The first submit therefore died with::

    NameError: name 'pp' is not defined

which names neither the cause nor the remedy. Three defects were tangled together here:

1. No ``import pp`` anywhere in the module, so ``server`` could only ever raise NameError.
2. ``nornir_pools.__ParallelPythonAvailable`` was initialised False and assigned False in
   the ImportError branch, and never assigned True. It reported "unavailable" even with a
   working backend, because importing ``parallelpythonpool`` succeeds whether or not ``pp``
   is installed -- so the import proved nothing.
3. ``add_task`` called ``IncrementActiveJobCount()`` *before* touching ``self.server``. The
   failed submit left the count at 1 for a task that never existed, and shutdown then hung
   printing "Waiting on pool: Pool ... with 1 active tasks". That hang is worse than the
   original NameError, since it needs a kill rather than a traceback.

The gated entry point ``GetGlobalClusterPool`` was never affected: it falls back to the
local machine pool. ``GetParallelPythonPool(name)`` bypasses that gate, which is how the
NameError was reachable.
"""
from __future__ import annotations

import importlib.util
import unittest
from unittest import mock

import nornir_pools
import nornir_pools.parallelpythonpool as ppp

PP_INSTALLED = importlib.util.find_spec('pp') is not None


class TestAvailabilityIsHonest(unittest.TestCase):

    def test_the_module_reports_whether_pp_is_importable(self):
        self.assertEqual(ppp.ParallelPythonAvailable, PP_INSTALLED)

    def test_the_package_flag_agrees_with_the_module(self):
        """The flag used to be hardcoded False regardless of the backend."""
        self.assertEqual(nornir_pools.IsParallelPythonAvailable(),
                         ppp.ParallelPythonAvailable)

    def test_the_module_imports_even_without_the_backend(self):
        """Availability is a query, not an import failure; the class stays introspectable."""
        self.assertTrue(hasattr(ppp, 'ParallelPythonProcess_Pool'))


class TestMissingBackendRaisesSomethingUseful(unittest.TestCase):

    def setUp(self):
        # Force the unavailable branch so this runs the same way on a cluster host.
        patcher = mock.patch.object(ppp, 'ParallelPythonAvailable', False)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.pool = ppp.ParallelPythonProcess_Pool(name='test-missing-backend')

    def test_server_raises_runtime_error(self):
        with self.assertRaises(RuntimeError) as ctx:
            _ = self.pool.server

        message = str(ctx.exception)
        self.assertIn('Parallel Python is not available', message)
        self.assertIn("'pp' distribution is not installed", message)
        # The remedy matters as much as the diagnosis.
        self.assertIn('GetGlobalLocalMachinePool', message)

    def test_server_does_not_raise_name_error(self):
        with self.assertRaises(RuntimeError):
            _ = self.pool.server

    def test_add_task_raises_runtime_error(self):
        with self.assertRaises(RuntimeError):
            self.pool.add_task('t', str, 1)

    def test_add_process_raises_runtime_error(self):
        with self.assertRaises(RuntimeError):
            self.pool.add_process('t', 'echo hi')


class TestFailedSubmitDoesNotLeakTheJobCount(unittest.TestCase):
    """The leak turned a clean error into a shutdown hang."""

    def setUp(self):
        patcher = mock.patch.object(ppp, 'ParallelPythonAvailable', False)
        patcher.start()
        self.addCleanup(patcher.stop)

        self._starting_count = ppp.ActiveJobCount
        self.pool = ppp.ParallelPythonProcess_Pool(name='test-no-leak')

    def test_add_task_leaves_the_count_untouched(self):
        with self.assertRaises(RuntimeError):
            self.pool.add_task('t', str, 1)

        self.assertEqual(ppp.ActiveJobCount, self._starting_count)

    def test_add_process_leaves_the_count_untouched(self):
        with self.assertRaises(RuntimeError):
            self.pool.add_process('t', 'echo hi')

        self.assertEqual(ppp.ActiveJobCount, self._starting_count)

    def test_repeated_failures_do_not_accumulate(self):
        for _ in range(5):
            with self.assertRaises(RuntimeError):
                self.pool.add_task('t', str, 1)

        self.assertEqual(ppp.ActiveJobCount, self._starting_count)

    def test_num_active_tasks_stays_zero_so_wait_completion_can_return(self):
        with self.assertRaises(RuntimeError):
            self.pool.add_task('t', str, 1)

        # A non-zero count here is what made wait_completion block forever.
        self.assertEqual(self.pool.num_active_tasks, self._starting_count)

    def test_a_submit_failure_after_the_server_exists_also_unwinds(self):
        """The unwind must cover server.submit raising, not just a missing backend."""
        broken = mock.Mock()
        broken.submit.side_effect = OSError('cluster node refused the job')

        pool = ppp.ParallelPythonProcess_Pool(name='test-submit-failure')
        pool._server = broken
        starting = ppp.ActiveJobCount

        with self.assertRaises(OSError):
            pool.add_task('t', str, 1)

        self.assertEqual(ppp.ActiveJobCount, starting)


class TestGatedEntryPointStillFallsBack(unittest.TestCase):
    """GetGlobalClusterPool was always safe; keep it that way."""

    def test_global_cluster_pool_falls_back_when_unavailable(self):
        if nornir_pools.IsParallelPythonAvailable():
            self.skipTest('pp is installed, so no fallback is expected')

        pool = nornir_pools.GetGlobalClusterPool()

        self.assertIsNotNone(pool)
        self.assertNotIsInstance(pool, ppp.ParallelPythonProcess_Pool)


if __name__ == '__main__':
    unittest.main()
