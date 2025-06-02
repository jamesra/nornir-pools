'''
Created on Apr 17, 2014

@author: u0490822
'''

import subprocess
from typing import Callable

import nornir_pools
import nornir_pools.processpool
import nornir_pools.task
from . import poolbase


class SerialPool(poolbase.PoolBase):
    '''
    Unified interface for the process and multithreading pools allowing both threads and processes to be launched from the same pool.
    '''

    @property
    def _process_pool(self):
        if self._ppool is None:
            self._ppool = nornir_pools.GetProcessPool(self.Name + " process pool", self._num_threads)

        return self._ppool

    def get_active_nodes(self):
        return ["localhost"]

    @property
    def num_active_tasks(self) -> int:
        return 1

    def __init__(self, num_threads, *args, **kwargs):
        '''
        Constructor
        '''

        self._num_threads = num_threads
        self._ppool = None
        # self._name = pool_name
        super(SerialPool, self).__init__(*args, **kwargs)

    def add_task(self, name: str, func: Callable, *args, **kwargs) -> nornir_pools.task.Task:
        retval = func(*args, **kwargs)
        return nornir_pools.task.SerialTask(name, retval, *args, **kwargs)

    def add_process(self, name: str, func: Callable, *args, **kwargs) -> nornir_pools.task.TaskWithEvent:
        SingleParameterProc = subprocess.Popen(func + " && exit", shell=True, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)

        entry = nornir_pools.processpool.ProcessTask(name, func, *args, **kwargs)
        entry.returned_value = SingleParameterProc.communicate(input)
        entry.stdoutdata = entry.returned_value[0].decode('utf-8')
        entry.stderrdata = entry.returned_value[1].decode('utf-8')
        entry.returncode = SingleParameterProc.returncode
        entry.completed.set()

        return entry

        # return self._process_pool.add_process(name, func, *args, **kwargs)

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""
        if not self._ppool is None:
            self._process_pool.wait_completion()

        return

    def shutdown(self):
        return
