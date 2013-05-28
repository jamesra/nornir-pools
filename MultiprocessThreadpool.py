# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import multiprocessing
import threading
# import copy_reg
# import types
import traceback
import logging
import os
import Pools.task

from threading import Lock

# import Pools

JobCountLock = Lock();
ActiveJobCount = 0;

def IncrementActiveJobCount():
    global JobCountLock
    global ActiveJobCount
    JobCountLock.acquire(True);
    ActiveJobCount = ActiveJobCount + 1
    JobCountLock.release();

def DecrementActiveJobCount():
    global JobCountLock
    global ActiveJobCount
    JobCountLock.acquire(True);
    ActiveJobCount = ActiveJobCount - 1
    JobCountLock.release();

def PrintJobsCount():
    global ActiveJobCount
    JobQText = "Jobs Queued: " + str(ActiveJobCount);
    JobQText = ('\b' * 40) + JobQText + ('.' * (40 - len(JobQText)));
    Pools.pprint (JobQText);

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
    DecrementActiveJobCount();
    PrintJobsCount();


class MultiprocessThreadTask():

    def __init__(self, name, asyncresult, logger, args, kwargs):

        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.asyncresult = asyncresult
        self.logger = logger


    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        retval = self.asyncresult.get();
        if self.asyncresult.successful():
            return retval;
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            return None;

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.asyncresult.wait();
        if self.asyncresult.successful():
            return
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            return None;

class MultiprocessThread_Pool:

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads = None):

        self.shutdown_event = threading.Event();
        self.logger = logging.getLogger('Multithreading Pool');
        self.logger.warn("Creating Multithreading Pool");

        # self.manager =  multiprocessing.Manager();
        self.tasks = multiprocessing.Pool();
        # for _ in range(num_threads): Worker(self.tasks,self.shutdown_event)

    def __del__(self):

        self.tasks.close()
        self.tasks.join()


    def add_task(self, name, func, *args, **kwargs):

        """Add a task to the queue"""

        IncrementActiveJobCount();
        PrintJobsCount();
        task = self.tasks.apply_async(func, args, kwargs, callback = callback);
        return MultiprocessThreadTask(name, task, self.logger, args, kwargs);


    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        self.tasks.close()
        self.tasks.join()
        self.tasks = multiprocessing.Pool();



