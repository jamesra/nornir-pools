'''
Created on Apr 17, 2014

@author: u0490822
'''

import nornir_pools
import nornir_shared.misc
import logging
from typing import Callable
from . import poolbase


class LocalMachinePool(poolbase.PoolBase):
    '''
    Unified interface for the process and multithreading pools allowing both threads and processes to be launched from the same pool.
    '''

    @property
    def queued_tasks(self) -> int:
        total = 0
        if self._mtpool is not None:
            total += self._mtpool.queued_tasks
        if self._ppool is not None:
            total += self._ppool.queued_tasks
        return total

    @property
    def active_tasks(self) -> int:
        total = 0
        if self._mtpool is not None:
            total += self._mtpool.active_tasks
        if self._ppool is not None:
            total += self._ppool.active_tasks
        return total

    @property
    def num_active_tasks(self):
        total = 0
        if self._mtpool is not None:
            total += self._mtpool.num_active_tasks

        if self._ppool is not None:
            total += self._ppool.num_active_tasks

        return total

    @property
    def _multithreading_pool(self):
        if self._mtpool is None:
            nornir_shared.misc.StartMultiprocessLoggingListener(level=logging.getLogger().getEffectiveLevel())

            if self.is_global:
                self._mtpool = nornir_pools.GetGlobalMultithreadingPool()
            else:
                self._mtpool = nornir_pools.GetMultithreadingPool(self.name + " multithreading pool", self._num_threads)

        return self._mtpool

    @property
    def _process_pool(self):
        if self._ppool is None:
            if self.is_global:
                self._ppool = nornir_pools.GetGlobalProcessPool()
            else:
                self._ppool = nornir_pools.GetProcessPool(self.name + " process pool", self._num_threads)

        return self._ppool

    def get_active_nodes(self):
        return ["localhost"]

    def __init__(self, name: str, num_workers: int | None = None, is_global=False, *args, **kwargs):
        '''
        Constructor
        '''

        num_workers = nornir_pools.ApplyOSThreadLimit(num_workers)

        self._num_threads = num_workers

        self.is_global = is_global
        self._mtpool = None
        self._ppool = None
        super(LocalMachinePool, self).__init__(name=name, *args, **kwargs)

    def add_task(self, name, func, *args, **kwargs) -> nornir_pools.task.Task:
        return self._multithreading_pool.add_task(name, func, *args, **kwargs)

    def warm(self, func: Callable | None = None) -> None:
        """Spawn process workers (and run *func* once each) before the first real task."""
        self._multithreading_pool.warm(func)

    def add_process(self, name, func, *args, **kwargs) -> nornir_pools.task.TaskWithEvent:
        return self._process_pool.add_process(name, func, *args, **kwargs)

    def wait_completion(self, timeout: float | None = None):
        """Wait for completion of all the tasks in the queue.

        :param timeout: Seconds to wait before giving up. ``None`` waits indefinitely.
            Forwarded to the multiprocessing pool; the process pool has no timeout
            parameter and is waited on only after the multiprocessing pool returns.
        :raises TimeoutError: If *timeout* elapses with multiprocessing tasks still
            registered. Their ids are named in the message.
        """
        if self._mtpool is not None:
            self._mtpool.wait_completion(timeout=timeout)

        if self._ppool is not None:
            self._ppool.wait_completion()

    def shutdown(self):
        # Do not wait unbounded here. MultiprocessThreadPool.shutdown already waits with
        # its own budget and terminates workers on timeout; a second unbounded wait in
        # front of that would reintroduce the hang ClosePools(timeout=...) is meant to
        # convert into a TimeoutError.
        if self._mtpool is not None:
            self._mtpool.shutdown()
            self._mtpool = None

        if self._ppool is not None:
            self._ppool.shutdown()
            self._ppool = None
