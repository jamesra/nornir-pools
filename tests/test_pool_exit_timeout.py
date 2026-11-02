"""Shutdown does not wait forever for idle workers that fail to exit.

On srv (RC3, 2026-11-02) every task had finished, but ~128 idle forkserver workers hung
in their queue feeder-thread join at exit, and ``pool.join()`` waited 10+ minutes until
the process was interrupted. Shutdown now waits ``NORNIR_POOL_EXIT_TIMEOUT`` seconds
(default 60) for the workers, then terminates them; nothing is lost because the tasks
have already completed.
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

import nornir_pools
from nornir_pools import _test_pool_tasks, multiprocessthreadpool


class TestPoolExitTimeout(unittest.TestCase):

    def _pool(self, name: str):
        pool = nornir_pools.GetMultithreadingPool(name, num_threads=2)
        for task in [pool.add_task(str(i), _test_pool_tasks.GetEnvironment, 'PATH') for i in range(4)]:
            task.wait_return()
        return pool

    def test_workers_that_exit_are_joined_normally(self):
        pool = self._pool('exit-timeout-normal')
        with mock.patch.object(pool, 'terminate_workers', wraps=pool.terminate_workers) as terminate:
            pool.shutdown()

        terminate.assert_not_called()

    def test_workers_that_do_not_exit_are_terminated(self):
        pool = self._pool('exit-timeout-stuck')
        with mock.patch.object(pool, '_workers_exit_within', return_value=False), \
                mock.patch.object(pool, 'terminate_workers', wraps=pool.terminate_workers) as terminate:
            pool.shutdown()

        terminate.assert_called_once()

    def test_exit_timeout_from_environment(self):
        for raw, expected in (('', 60.0), ('5', 5.0), ('-1', 0.0), ('soon', 60.0)):
            with self.subTest(raw=raw), mock.patch.dict(os.environ, {multiprocessthreadpool.EXIT_TIMEOUT_ENV: raw}):
                self.assertEqual(multiprocessthreadpool._exit_timeout(), expected)


if __name__ == '__main__':
    unittest.main()
