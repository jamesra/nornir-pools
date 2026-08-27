"""ParallelPythonProcess_Pool must satisfy the IPool interface.

The pool declared only the legacy ``ActiveTasks`` property and never the
abstract ``IPool.num_active_tasks``, so ABC instantiation raised TypeError and
the class was unusable in every configuration.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import unittest

import nornir_pools
import nornir_pools.parallelpythonpool as pp
from nornir_pools.ipool import IPool

# Intermediate bases in this module are abstract on purpose; concrete pools live
# elsewhere and must implement the whole interface.
_ABSTRACT_BASE_MODULE = 'nornir_pools.poolbase'


class TestParallelPythonPoolSatisfiesIPool(unittest.TestCase):

    def test_no_abstract_methods_remain(self) -> None:
        self.assertEqual(
            set(pp.ParallelPythonProcess_Pool.__abstractmethods__), set(),
            "ParallelPythonProcess_Pool still has unimplemented abstract members")

    def test_pool_can_be_instantiated(self) -> None:
        pool = pp.ParallelPythonProcess_Pool(name="test-pp-pool")
        self.assertIsInstance(pool, IPool)

    def test_num_active_tasks_reports_the_job_count(self) -> None:
        prev = pp.ActiveJobCount
        pp.ActiveJobCount = 0
        try:
            pool = pp.ParallelPythonProcess_Pool(name="test-pp-pool")
            self.assertEqual(pool.num_active_tasks, 0)

            pp.IncrementActiveJobCount()
            self.assertEqual(pool.num_active_tasks, 1)

            pp.DecrementActiveJobCount()
            self.assertEqual(pool.num_active_tasks, 0)
        finally:
            pp.ActiveJobCount = prev

    def test_legacy_ActiveTasks_alias_still_agrees(self) -> None:
        """nornir_buildmanager.operations.tile throttles on this name."""
        prev = pp.ActiveJobCount
        pp.ActiveJobCount = 0
        try:
            pool = pp.ParallelPythonProcess_Pool(name="test-pp-pool")
            pp.IncrementActiveJobCount()
            self.assertEqual(pool.ActiveTasks, pool.num_active_tasks)
            self.assertEqual(pool.ActiveTasks, 1)
        finally:
            pp.ActiveJobCount = prev


class TestEveryConcretePoolImplementsIPool(unittest.TestCase):
    """Guard the whole family, so a new pool cannot repeat this gap.

    ParallelPythonProcess_Pool went unnoticed because nothing asserted that a
    pool is instantiable; the failure only appears at the call site.
    """

    def _concrete_pool_classes(self) -> dict[str, type]:
        found: dict[str, type] = {}
        for module_info in pkgutil.iter_modules(nornir_pools.__path__):
            try:
                module = importlib.import_module(f'nornir_pools.{module_info.name}')
            except Exception:
                continue
            for _, cls in inspect.getmembers(module, inspect.isclass):
                if not issubclass(cls, IPool) or cls is IPool:
                    continue
                if cls.__module__ == _ABSTRACT_BASE_MODULE:
                    continue
                found[f'{cls.__module__}.{cls.__name__}'] = cls
        return found

    def test_discovery_found_the_known_pools(self) -> None:
        """Fail loudly if discovery silently stops seeing anything."""
        names = self._concrete_pool_classes()
        self.assertIn('nornir_pools.parallelpythonpool.ParallelPythonProcess_Pool', names)
        self.assertIn('nornir_pools.serialpool.SerialPool', names)
        self.assertGreaterEqual(len(names), 5)

    def test_no_concrete_pool_has_unimplemented_members(self) -> None:
        offenders = {
            name: sorted(cls.__abstractmethods__)
            for name, cls in self._concrete_pool_classes().items()
            if cls.__abstractmethods__
        }
        self.assertEqual(offenders, {}, f"Concrete pools with unimplemented members: {offenders}")


if __name__ == "__main__":
    unittest.main()
