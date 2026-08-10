"""Tests for nornir_pools queued/active counters and pool_load reporting."""
from __future__ import annotations

import time
import unittest
from unittest import mock

import nornir_pools
from nornir_pools.instrumented_executor import InstrumentedThreadPoolExecutor


class TestThreadPoolLoadCounters(unittest.TestCase):
    def tearDown(self) -> None:
        nornir_pools.CloseThreadPools()

    def test_queued_and_active_split_during_work(self) -> None:
        pool = nornir_pools.GetThreadPool("unit-pool-load", num_threads=1)
        started = time.time()
        gate = {"go": False}

        def slow(_n: int) -> int:
            while not gate["go"] and time.time() - started < 5:
                time.sleep(0.01)
            return 1

        with mock.patch.object(pool, "TryReportPoolLoad"):
            t1 = pool.add_task("a", slow, 1)
            t2 = pool.add_task("b", slow, 2)
            # Allow the single worker to dequeue one task.
            deadline = time.time() + 2
            while pool.active_tasks < 1 and time.time() < deadline:
                time.sleep(0.01)
            self.assertGreaterEqual(pool.active_tasks, 1)
            self.assertGreaterEqual(pool.queued_tasks, 1)
            self.assertEqual(pool.num_active_tasks, pool.queued_tasks + pool.active_tasks)
            gate["go"] = True
            t1.wait()
            t2.wait()
            pool.wait_completion()
            self.assertEqual(pool.queued_tasks, 0)
            self.assertEqual(pool.active_tasks, 0)


class TestInstrumentedExecutor(unittest.TestCase):
    def test_reports_pool_load_around_submit(self) -> None:
        with mock.patch("nornir_pools.instrumented_executor.report_pool_load") as report:
            with InstrumentedThreadPoolExecutor("unit-executor", max_workers=2) as ex:
                f = ex.submit(lambda: 42)
                self.assertEqual(f.result(timeout=2), 42)
        self.assertGreaterEqual(report.call_count, 2)
        names = {c.kwargs.get("name") or (c.args[0] if c.args else None) for c in report.call_args_list}
        # positional name is first arg
        names |= {c.args[0] for c in report.call_args_list if c.args}
        self.assertIn("unit-executor", names)


if __name__ == "__main__":
    unittest.main()
