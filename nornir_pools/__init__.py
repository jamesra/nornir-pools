''' 

The use pattern for pools is:

1. Create a pool
2. add a task or process to the pool
3. save the task object returned
4. call wait or wait_return on the task object to fetch the output or raise exceptions

Steps 3 and 4 can be skipped if output is not required.  In this case wait_completion can be called on the pool to delay until all tasks have completed.  Note that in this pattern exceptions may be lost.
 
Pool Creation
-------------

Pool creation functions share a common signature

.. py:function:: Get<X>Pool([Poolname=None, num_threads=None)
   
   Return a pool of X type, listed below.  Repeated calls using the same name returns the same pool
   
   :param str Poolname: Name of the pool to get or create.  Passing "None" returns the global pool
   :param int num_threads: Number of tasks allowed to execute concurrently.  Not honored by all pools at this time
   
   :returns: object derived from PoolBase
   :rtype: PoolBase
    
.. autofunction:: GetThreadPool
.. autofunction:: GetMultithreadingPool
.. autofunction:: GetProcessPool
.. autofunction:: GetParallelPythonPool

Global pools
------------

Most callers will not care about getting a specific pool.  These functions always return the same pool.

.. autofunction:: GetGlobalThreadPool
.. autofunction:: GetGlobalMultithreadingPool
.. autofunction:: GetGlobalProcessPool
.. autofunction:: GetGlobalClusterPool

 
Pool Objects
------------
.. automodule:: nornir_pools.poolbase
   :members:
   
Task Objects
------------
.. autoclass:: nornir_pools.task.Task

Pool Destruction
----------------

It is not necessary to perform any cleanup.  Functions to delete pools would not be hard to add.  ClosePools is called automatically at script termination by atexit

.. autofunction:: nornir_pools.ClosePools

'''

import nornir_pools.processpool as processpool
import nornir_pools.threadpool as threadpool
import nornir_pools.multiprocessthreadpool as multiprocessthreadpool
import nornir_pools.parallelpythonpool as parallelpythonpool
import atexit
import os
import sys
import datetime

import logging

dictKnownPools = {}


def GetThreadPool(Poolname=None, num_threads=None):
    '''Get or create a thread pool using vanilla python threads'''
    return __CreatePool(threadpool.Thread_Pool, Poolname, num_threads)


def GetMultithreadingPool(Poolname=None, num_threads=None):
    '''Get or create a thread pool to execute threads in other processes using the multiprocessing library'''
    return __CreatePool(multiprocessthreadpool.MultiprocessThread_Pool, Poolname, num_threads)


def GetProcessPool(Poolname=None, num_threads=None):
    '''Get or create a pool to invoke shell command processes'''
    return __CreatePool(processpool.Process_Pool, Poolname, num_threads)


def GetParallelPythonPool(Poolname=None, num_threads=None):
    '''Get or create a pool to invoke shell command processes on a cluster using parallel python'''
    return __CreatePool(parallelpythonpool.ParallelPythonProcess_Pool, Poolname, num_threads)


def __CreatePool(poolclass, Poolname=None, num_threads=None):

    global dictKnownPools

    if Poolname is None:
        return GetGlobalMultithreadingPool()

    if Poolname in dictKnownPools:
        pool = dictKnownPools[Poolname]
        assert(pool.__class__ == poolclass)
        return dictKnownPools[Poolname]

    pool = poolclass(num_threads)
    pool.Name = Poolname

    dictKnownPools[Poolname] = pool

    return pool


def GetGlobalProcessPool():
    return GetProcessPool("Global local process pool")


def GetGlobalClusterPool():
    return GetParallelPythonPool("Global cluster pool")


def GetGlobalThreadPool():
    return GetThreadPool("Global local thread pool")


def GetGlobalMultithreadingPool():
    '''Threads based on pythons multiprocess library which places each thread in a seperate process to avoid the GIL'''
    return GetMultithreadingPool("Global multithreading pool")

# ToPreventFlooding the output I only write pool size every five seconds when running under ECLIPSE
__LastConsoleWrite = datetime.datetime.utcnow()


def __EclipseConsoleWrite(s, newline=False):
    s = s.replace('\b', '');
    s = s.replace('.', '');
    s = s.strip();

    if newline:
        s = s + '\n'

    sys.stdout.write(s)


def __PrintProgressUpdateEclipse(s):
    global __LastConsoleWrite

    now = datetime.datetime.utcnow()
    delta = now - __LastConsoleWrite

    if delta.seconds < 10:
        return

    __EclipseConsoleWrite(s, newline=True)
    __LastConsoleWrite = datetime.datetime.utcnow()


def __ConsoleWrite(s, newline=False):
    if newline:
        s = s + '\n'

    sys.stdout.write(s)


def PrintProgressUpdate(s):
    if  'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return

    __ConsoleWrite(s)


def sprint(s):
    """ Thread-safe print fucntion """
    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=True)
    else:
        __ConsoleWrite(s, newline=True)


def pprint(s):
    """ Thread-safe print fucntion, no newline """

    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=False)
    else:
        __ConsoleWrite(s, newline=False)


def ClosePools():
    global dictKnownPools

    for (key, pool) in dictKnownPools.items():
        sprint("Waiting on pool: " + key)
        pool.Shutdown()

    dictKnownPools.clear()

    # knownPoolKeys = dictKnownPools.keys()
    # for key in knownPoolKeys:
    #    del dictKnownPools[key]

atexit.register(ClosePools)
