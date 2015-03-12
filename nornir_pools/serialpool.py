'''
Created on Apr 17, 2014

@author: u0490822
'''

import nornir_pools
from . import poolbase
import nornir_pools.task

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

    def __init__(self, num_threads):
        '''
        Constructor
        '''

        self._num_threads = num_threads 
        self._ppool = None
        # self._name = pool_name
 

    def add_task(self, name, func, *args, **kwargs):
        retval = func(*args, **kwargs)
        return nornir_pools.task.SerialTask(name, retval, *args, **kwargs)
        

    def add_process(self, name, func, *args, **kwargs):
        return self._process_pool.add_process(name, func, *args, **kwargs)

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""
        if not self._ppool is None:
            self._process_pool.wait_completion()
             
        return

    def shutdown(self):
        return