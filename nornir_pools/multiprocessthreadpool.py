# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import multiprocessing
import multiprocessing.pool
import subprocess
import socket
import threading
import logging
# import copy_reg
# import types
import traceback 
import os
import nornir_pools.task

import nornir_pools as pools 

from threading import Lock 

# import pools

JobCountLock = Lock()
ActiveJobCount = 0

def IncrementActiveJobCount():
    #global JobCountLock
    global ActiveJobCount
    #JobCountLock.acquire(True)
    ActiveJobCount += 1
    #JobCountLock.release()


def DecrementActiveJobCount():
    #global JobCountLock
    global ActiveJobCount
    #JobCountLock.acquire(True)
    ActiveJobCount -= 1
    #JobCountLock.release()


def PrintJobsCount():
    global ActiveJobCount
    JobQText = "Jobs Queued: " + str(ActiveJobCount)
    JobQText = ('\b' * 40) + JobQText + ('.' * (40 - len(JobQText)))
    pools._PrintProgressUpdate (JobQText)


def _pickle_method(method):
    func_name = method.__func__.__name__
    obj = method.__self__
    cls = method.__self__.__class__
    if func_name.startswith('__') and not func_name.endswith('__'):  # deal with mangled names
        cls_name = cls.__name__.lstrip('_')
        func_name = '_' + cls_name + func_name
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.__mro__:
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)

# copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)


class NoDaemonProcess(multiprocessing.Process):

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)
     
#     def run(self):
#         '''
#         Method to be run in sub-process; can be overridden in sub-class
#         '''
#         pools.start_profiling()
#         retval = super(NoDaemonProcess, self).run() 
#         return retval
# #         
#     def terminate(self):
# #         '''
# #         Terminate process; sends SIGTERM signal or uses TerminateProcess()
# #         '''
#         pools.end_profiling()
#         return super(NoDaemonProcess, self).terminate() 
                

class NonDaemonPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


class MultiprocessThreadTask(nornir_pools.task.Task):
    
    @property
    def logger(self):
        return logging.getLogger(__name__) 
        
        
    def callback(self, result):
        DecrementActiveJobCount()
        PrintJobsCount()
        self.set_completion_time()
        #self.logger.info("%s" % str(self.__str__()))
        pools._sprint("%s" % str(self.__str__()))
    
    def callbackontaskfail(self, task):
        '''This is manually invoked by the task when a thread fails to complete'''
        DecrementActiveJobCount()
        PrintJobsCount()
        self.set_completion_time()

    def __init__(self, name, asyncresult, *args, **kwargs):

        super(MultiprocessThreadTask, self).__init__(name, *args, **kwargs)
        #self.args = args
        #self.kwargs = kwargs
        self.asyncresult = asyncresult 

    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        retval = self.asyncresult.get()
        if self.asyncresult.successful():
            #self.logger.info("Multiprocess successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs)
            return retval
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            self.callbackontaskfail(self)
            return None

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.asyncresult.wait()
        if self.asyncresult.successful():
            return
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            self.callbackontaskfail(self)
            self.asyncresult.get()  # This should cause the original exception to be raised according to multiprocess documentation
            return None

    @property
    def iscompleted(self):
        return self.asyncresult.ready()


class MultiprocessThread_Pool(nornir_pools.poolbase.PoolBase):

    """Pool of threads consuming tasks from a queue"""

    @property
    def tasks(self):
        if self._tasks is None:
            self._tasks = NonDaemonPool(maxtasksperchild=None)

        return self._tasks

    def __init__(self, num_threads=None):
        self._tasks = None

    def shutdown(self):
        self.wait_completion()

    def add_task(self, name, func, *args, **kwargs):

        """Add a task to the queue"""

        
        # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
        # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
        # This hangs the caller if they wait on the task.
        
        retval_task = MultiprocessThreadTask(name, None, args, kwargs)
        retval_task.asyncresult = self.tasks.apply_async(func, args, kwargs, callback=retval_task.callback)
        if retval_task.asyncresult is None:
            raise ValueError("apply_async returned None instead of an asyncresult object")
        
        IncrementActiveJobCount()
        PrintJobsCount()
        
        return retval_task

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        if hasattr(self, 'tasks'):
            self.tasks.close()
            self.tasks.join()
            self._tasks = None
