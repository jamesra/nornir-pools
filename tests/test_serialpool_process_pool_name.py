"""SerialPool._process_pool read self.Name; PoolBase only exposes lowercase name.

    self._ppool = nornir_pools.GetProcessPool(self.Name + " process pool", ...)
                                              ^^^^^^^^^  # type: ignore[attr-defined]

``PoolBase`` defines a ``name`` property over ``self._name`` and has no ``Name`` at all, so
forcing the property raised::

    AttributeError: 'SerialPool' object has no attribute 'Name'

The ``# type: ignore[attr-defined]`` on that line silenced the checker on the one line it
would have caught, which is how this survived.

Reachability: the branch is currently dead, and dead in a self-referential way. ``_ppool``
starts None in the constructor and is only ever assigned non-None *inside this property*.
``wait_completion`` is the sole live caller and guards on ``_ppool is not None``, so it can
never enter. ``add_process`` runs inline and its ``_process_pool`` call is commented out.
The property is vestigial on a pool whose entire purpose is running serially.

Fixed rather than closed as unreachable because it is a one-word correction, and a landmine
that only fires the first time someone wires up the process path is worse than one that
fires now.
"""
from __future__ import annotations

import unittest

import nornir_pools.poolbase as poolbase
import nornir_pools.serialpool as serialpool


class TestPoolBaseNaming(unittest.TestCase):
    """Pin the attribute this property has to agree with."""

    def test_pool_base_exposes_lowercase_name(self):
        self.assertTrue(hasattr(poolbase.PoolBase, 'name'))

    def test_pool_base_has_no_capitalised_name(self):
        self.assertFalse(hasattr(poolbase.PoolBase, 'Name'))

    def test_a_serial_pool_instance_has_no_capitalised_name(self):
        pool = serialpool.SerialPool(name='test-naming')

        self.assertEqual(pool.name, 'test-naming')
        self.assertFalse(hasattr(pool, 'Name'))


class TestProcessPoolPropertyResolves(unittest.TestCase):
    """The regression: forcing the property used to raise AttributeError."""

    def setUp(self):
        self.pool = serialpool.SerialPool(name='test-process-pool')
        self.addCleanup(self.pool.shutdown)

    def test_the_property_does_not_raise_attribute_error(self):
        created = []

        def fake_get_process_pool(poolname, num_threads=None):
            created.append(poolname)
            return object()

        original = serialpool.nornir_pools.GetProcessPool
        serialpool.nornir_pools.GetProcessPool = fake_get_process_pool
        try:
            result = self.pool._process_pool
        finally:
            serialpool.nornir_pools.GetProcessPool = original

        self.assertIsNotNone(result)
        self.assertEqual(created, ['test-process-pool process pool'])

    def test_the_pool_name_is_derived_from_the_lowercase_attribute(self):
        seen = []

        def fake_get_process_pool(poolname, num_threads=None):
            seen.append(poolname)
            return object()

        pool = serialpool.SerialPool(name='Weird Name 123')
        self.addCleanup(pool.shutdown)

        original = serialpool.nornir_pools.GetProcessPool
        serialpool.nornir_pools.GetProcessPool = fake_get_process_pool
        try:
            _ = pool._process_pool
        finally:
            serialpool.nornir_pools.GetProcessPool = original

        self.assertEqual(seen, ['Weird Name 123 process pool'])

    def test_the_result_is_cached(self):
        calls = []

        def fake_get_process_pool(poolname, num_threads=None):
            calls.append(poolname)
            return object()

        original = serialpool.nornir_pools.GetProcessPool
        serialpool.nornir_pools.GetProcessPool = fake_get_process_pool
        try:
            first = self.pool._process_pool
            second = self.pool._process_pool
        finally:
            serialpool.nornir_pools.GetProcessPool = original

        self.assertIs(first, second)
        self.assertEqual(len(calls), 1, 'the pool should only be built once')

    def test_the_source_no_longer_says_self_dot_capital_name(self):
        """Guards the spelling directly, since the branch is otherwise unreachable."""
        import inspect

        source = inspect.getsource(serialpool.SerialPool)
        # Comments discuss the old spelling by name, so scan code lines only.
        code = '\n'.join(line.split('#', 1)[0]
                         for line in source.splitlines())

        self.assertNotIn('self.Name', code)
        self.assertIn('self.name + " process pool"', code)


class TestNormalSerialUseIsUnaffected(unittest.TestCase):
    """SerialPool runs inline; the process pool is never built in ordinary use."""

    def setUp(self):
        self.pool = serialpool.SerialPool(name='test-serial-normal')
        self.addCleanup(self.pool.shutdown)

    def test_add_task_runs_inline_and_returns_the_value(self):
        task = self.pool.add_task('double', lambda x: x * 2, 21)

        self.assertEqual(task.wait_return(), 42)

    def test_wait_completion_does_not_build_a_process_pool(self):
        self.pool.add_task('noop', str, 1)

        self.pool.wait_completion()

        self.assertIsNone(self.pool._ppool,
                          'wait_completion must stay on its guarded path')

    def test_the_process_pool_starts_unbuilt(self):
        self.assertIsNone(self.pool._ppool)


if __name__ == '__main__':
    unittest.main()
