# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import multiprocessing
import multiprocessing.pool
import threading
# import copy_reg
# import types
import traceback
import logging
import os
import task

import nornir_pools as pools

from threading import Lock

# import pools

JobCountLock = Lock()
ActiveJobCount = 0


def IncrementActiveJobCount():
    global JobCountLock
    global ActiveJobCount
    JobCountLock.acquire(True)
    ActiveJobCount = ActiveJobCount + 1
    JobCountLock.release()


def DecrementActiveJobCount():
    global JobCountLock
    global ActiveJobCount
    JobCountLock.acquire(True)
    ActiveJobCount = ActiveJobCount - 1
    JobCountLock.release()


def PrintJobsCount():
    global ActiveJobCount
    JobQText = "Jobs Queued: " + str(ActiveJobCount)
    JobQText = ('\b' * 40) + JobQText + ('.' * (40 - len(JobQText)))
    pools.PrintProgressUpdate (JobQText)


def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
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

def callback(result):
    DecrementActiveJobCount()
    PrintJobsCount()


class NoDaemonProcess(multiprocessing.Process):

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NonDaemonPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


class MultiprocessThreadTask():

    def __init__(self, name, asyncresult, logger, args, kwargs):

        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.asyncresult = asyncresult
        self.logger = logger


    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        retval = self.asyncresult.get()
        if self.asyncresult.successful():
            return retval
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            return None

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.asyncresult.wait()
        if self.asyncresult.successful():
            return
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            return None

    @property
    def iscompleted(self):
        return self.asyncresult.ready()

class MultiprocessThread_Pool:

    """Pool of threads consuming tasks from a queue"""

    @property
    def tasks(self):
        if self._tasks is None:
            self._tasks = NonDaemonPool()

        return self._tasks

    def __init__(self, num_threads=None):
        self.shutdown_event = threading.Event()
        self.logger = logging.getLogger('Multithreading Pool')
        self.logger.warn("Creating Multithreading Pool")

        # self.manager =  multiprocessing.Manager()
        # self.tasks = multiprocessing.Pool()
        self._tasks = None
        # for _ in range(num_threads): Worker(self.tasks,self.shutdown_event)

    def Shutdown(self):
        self.wait_completion()


#     def __del__(self):
#
#         if hasattr(self, 'tasks'):
#             self.tasks.close()
#             self.tasks.join()
#             self._tasks = None


    def add_task(self, name, func, *args, **kwargs):

        """Add a task to the queue"""

        IncrementActiveJobCount()
        PrintJobsCount()
        task = self.tasks.apply_async(func, args, kwargs, callback=callback)
        return MultiprocessThreadTask(name, task, self.logger, args, kwargs)


    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        if hasattr(self, 'tasks'):
            self.tasks.close()
            self.tasks.join()
            self._tasks = None


