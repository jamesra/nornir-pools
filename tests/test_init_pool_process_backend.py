"""``init_pool_process`` has to pin forked workers to the NumPy backend.

A forked worker inherits the parent's already-imported modules, so a GPU parent hands its
child an active CuPy backend without any module-level import re-running.  The child then
initializes CUDA the first time it touches tile or transform work, inside a forked process --
which is exactly the configuration the CUDA runtime does not support.

nornir_imageregistration.computational_lib.ConfigureForkPoolWorker exists to correct that and
documents init_pool_process as its call site, so these tests pin the wiring: the call happens,
the parent process is unaffected, and a stack without nornir_imageregistration installed still
starts workers.  nornir_pools sits below nornir_imageregistration in the dependency order, so
that last property is a real constraint rather than defensive habit.
"""

from __future__ import annotations

import builtins
import unittest
from unittest import mock

import nornir_pools


class TestItConfiguresTheWorkerBackend(unittest.TestCase):

    def test_the_imageregistration_hook_is_called(self):
        with mock.patch('nornir_imageregistration.computational_lib.ConfigureForkPoolWorker') as configure:
            nornir_pools.init_pool_process()

        configure.assert_called_once_with()

    def test_the_lock_is_still_published_for_the_legacy_api(self):
        sentinel = object()

        nornir_pools.init_pool_process(the_lock=sentinel)

        self.assertIs(sentinel, nornir_pools.shared_lock)

    def test_the_hook_runs_even_when_no_logging_queue_is_supplied(self):
        """Queue logging is optional; skipping it must not skip the backend pin."""
        with mock.patch('nornir_imageregistration.computational_lib.ConfigureForkPoolWorker') as configure:
            nornir_pools.init_pool_process(logging_queue=None)

        configure.assert_called_once_with()


class TestTheParentProcessIsUnaffected(unittest.TestCase):
    """The initializer is importable and callable in the parent; there it must change nothing."""

    def test_calling_it_in_the_parent_leaves_the_active_backend_alone(self):
        import nornir_imageregistration

        before = nornir_imageregistration.GetActiveComputationLib()

        nornir_pools.init_pool_process()

        self.assertEqual(before, nornir_imageregistration.GetActiveComputationLib())


class TestItToleratesAStackWithoutImageRegistration(unittest.TestCase):
    """nornir_pools is the lower package: it must start workers without its consumer present."""

    def test_an_import_error_is_swallowed(self):
        real_import = builtins.__import__

        def refuse_imageregistration(name, *args, **kwargs):
            if name.startswith('nornir_imageregistration'):
                raise ImportError(f'pretending {name} is not installed')
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, '__import__', refuse_imageregistration):
            nornir_pools.init_pool_process()

    def test_queue_logging_is_still_configured_before_the_import_is_attempted(self):
        real_import = builtins.__import__

        def refuse_imageregistration(name, *args, **kwargs):
            if name.startswith('nornir_imageregistration'):
                raise ImportError(f'pretending {name} is not installed')
            return real_import(name, *args, **kwargs)

        queue = object()
        with mock.patch('nornir_pools.nornir_logging_misc.ConfigureWorkerQueueLogging') as configure_logging:
            with mock.patch.object(builtins, '__import__', refuse_imageregistration):
                nornir_pools.init_pool_process(logging_queue=queue, logging_level=10)

        configure_logging.assert_called_once_with(log_queue=queue, level=10)


if __name__ == '__main__':
    unittest.main()
