'''
Created on Apr 17, 2014

@author: u0490822
'''

import nornir_pools
from . import poolbase

class LocalMachinePool(poolbase.PoolBase):
    '''
    Unified interface for the process and multithreading pools allowing both threads and processes to be launched from the same pool.
    '''

    @property
    def _multithreading_pool(self):
        return nornir_pools.GetMultithreadingPool(self.Name + " multithreading pool", self._num_threads)

    @property
    def _process_pool(self):
        return nornir_pools.GetProcessPool(self.Name + " process pool", self._num_threads)

    def get_active_nodes(self):
        return ["localhost"]

    def __init__(self, num_threads):
        '''
        Constructor
        '''

        self._num_threads = num_threads
        # self._name = pool_name

        self._multithreadpool = None
        self._processpool = None

    def add_task(self, name, func, *args, **kwargs):
        return self._multithreading_pool.add_task(name, func, *args, **kwargs)

    def add_process(self, name, func, *args, **kwargs):
        return self._process_pool.add_process(name, func, *args, **kwargs)

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""
        self._multithreading_pool.wait_completion()
        self._process_pool.wait_completion()

    def shutdown(self):
        self.wait_completion()