''' 

nornir_pools aims to provide a consistent interface around four different multi-threading and clustering libraries available to Python.

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
   :members:

Pool Destruction
----------------

It is not necessary to perform any cleanup.  Functions to delete pools would not be hard to add.  ClosePools is called automatically at script termination by atexit

.. autofunction:: nornir_pools.ClosePools

Optimization
------------

On windows there is significant overhead to passing parameters to multiprocessing jobs.  To address this I added pickle overrides to objects being marshalled.  I also removed as many global initializations as I could from modules loaded by the tasks.

'''

import atexit
import os
import sys
import datetime
import warnings
import threading
import cProfile
import pstats
import glob
import shutil
import six
import logging

import nornir_pools.processpool
import nornir_pools.threadpool
import nornir_pools.multiprocessthreadpool
import nornir_pools.local_machine_pool
import nornir_pools.serialpool

__ParallelPythonAvailable = False

try:
    import nornir_pools.parallelpythonpool
except ImportError as e:
    __ParallelPythonAvailable = False
    pass

dictKnownPools = {}
 
def GetThreadPool(Poolname=None, num_threads=None):
    '''
    Get or create a specific thread pool using vanilla python threads    
    '''
    return __CreatePool(nornir_pools.threadpool.Thread_Pool, Poolname, num_threads)


def GetLocalMachinePool(Poolname=None, num_threads=None, is_global=False):

    return __CreatePool(nornir_pools.local_machine_pool.LocalMachinePool, Poolname, num_threads,is_global=is_global)


def GetMultithreadingPool(Poolname=None, num_threads=None):
    '''Get or create a specific thread pool to execute threads in other processes on the same computer using the multiprocessing library'''
    warnings.warn(DeprecationWarning("GetMultithreadingPool is deprecated.  Use GetLocalMachinePool instead"))
    return __CreatePool(nornir_pools.multiprocessthreadpool.MultiprocessThread_Pool , Poolname, num_threads)


def GetProcessPool(Poolname=None, num_threads=None):
    '''Get or create a specific pool to invoke shell command processes on the same computer using the subprocess module'''
    warnings.warn(DeprecationWarning("GetProcessPool is deprecated.  Use GetLocalMachinePool instead"))
    return __CreatePool(nornir_pools.processpool.Process_Pool, Poolname, num_threads)


def GetParallelPythonPool(Poolname=None, num_threads=None):
    '''Get or create a specific pool to invoke functions or shell command processes on a cluster using parallel python'''
    return __CreatePool(nornir_pools.parallelpythonpool.ParallelPythonProcess_Pool, Poolname, num_threads)


def GetSerialPool(Poolname=None, num_threads=None):
    '''
    Get or create a specific thread pool using vanilla python threads    
    '''
    if Poolname is None:
        raise ValueError("Must supply a pool name")
    return __CreatePool(nornir_pools.serialpool.SerialPool, Poolname, num_threads)


def __CreatePool(poolclass, Poolname=None, num_threads=None, *args, **kwargs):

    global dictKnownPools

    if Poolname is None:
        return GetGlobalLocalMachinePool()

    if Poolname in dictKnownPools:
        pool = dictKnownPools[Poolname]
        assert(pool.__class__ == poolclass)
        return dictKnownPools[Poolname]

    logging.warn("Creating %s pool of type %s" % (Poolname, poolclass))

    pool = poolclass(num_threads, *args, **kwargs)
    pool.Name = Poolname

    dictKnownPools[Poolname] = pool

    return pool

def GetGlobalSerialPool():
    '''
    Common pool for processes on the local machine
    '''
    return GetSerialPool(Poolname="Global")
    # return GetProcessPool("Global local process pool")

def GetGlobalProcessPool():
    '''
    Common pool for processes on the local machine
    '''
    return GetProcessPool(Poolname="Global process pool")
    # return GetProcessPool("Global local process pool")

def GetGlobalLocalMachinePool():
    '''
    Common pool for launching other processes for threads or executables.  Combines multithreading and process pool interface.
    '''

    return GetLocalMachinePool(Poolname="Global local machine pool", is_global=True)

def GetGlobalClusterPool():
    '''
    Get the common pool for placing tasks on the cluster
    '''
    if not __ParallelPythonAvailable:
        return GetGlobalLocalMachinePool()
        # raise Exception("Parallel python is not available")

    return GetParallelPythonPool("Global cluster pool")


def GetGlobalThreadPool():
    '''
    Common pool for thread based tasks
    '''
    return GetThreadPool("Global local thread pool")


def GetGlobalMultithreadingPool():
    '''
    Common pool for multithreading module tasks, threads run in different python processes to work around the global interpreter lock
    '''
    # return GetGlobalLocalMachinePool()
    return GetMultithreadingPool("Global multithreading pool")

# ToPreventFlooding the output I only write pool size every five seconds when running under ECLIPSE
__LastConsoleWrite = datetime.datetime.utcnow()


def __CleanOutputForEclipse(s):
    s = s.replace('\b', '');
    s = s.replace('.', '');
    s = s.strip();

    return s


def __EclipseConsoleWrite(s, newline=False):

    es = __CleanOutputForEclipse(s)    
    if newline:
        es = es + '\n'
 
    sys.stdout.write(es)
    
    
def __EclipseConsoleWriteError(s, newline=False):

    es = __CleanOutputForEclipse(s)    
    if newline:
        es = es + '\n'
 
    sys.stderr.write(es)
     

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
    
def __ConsoleWriteError(s, newline=False):
    if newline:
        s = s + '\n'

    sys.stderr.write(s)
    

def _PrintError(s):
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s)
        return

    __ConsoleWriteError(s, newline=True)


def _PrintWarning(s):
    if  'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return

    __ConsoleWrite(s, newline=True)


def _PrintProgressUpdate(s):
    if  'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return

    __ConsoleWrite(s)


def _sprint(s):
    """ Thread-safe print fucntion """
    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=True)
    else:
        __ConsoleWrite(s, newline=True)


def _pprint(s):
    """ Thread-safe print fucntion, no newline """

    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=False)
    else:
        __ConsoleWrite(s, newline=False)


profiler = None
profile_data_path = None

def GetAndCreateProfileDataPath():  

    profile_data_path = os.path.join(os.getcwd(), 'pool_profiles')
    #profile_data_path = os.path.join("C:\\Temp\\Testoutput\\PoolTestBase\\", 'pool_profiles')
    if not os.path.exists(profile_data_path):
        os.makedirs(profile_data_path)
        
    return profile_data_path

def GetAndCreateProfileDataFileName():  
    
    profile_data_path = GetAndCreateProfileDataPath()
        
    thread =  threading.current_thread()
    filename = "%d_%d.profile" % (os.getpid(), thread.ident)
    profile_data_file = os.path.join(profile_data_path, filename)
    return profile_data_file

def start_profiling():
    return
#     global profiler
#     
#     if not profiler is None:
#         #print("Profiler already initialized for pool")
#         return 
#     
#     profiler = cProfile.Profile()
#     profiler.enable()
#     atexit.register(end_profiling())

def end_profiling():
    return
#     global profiler
#     if not profiler is None:
#         profile_data_path = GetAndCreateProfileDataFileName()
#         profiler.dump_stats(profile_data_path)
#         profiler = None

def invoke_with_profiler(func, *args, **kwargs):
#    '''Launch a profiler for our function

    func_args = args

    start_profiling()
    func(*func_args, **kwargs)

def aggregate_profiler_data(output_path):
    return
#     profile_data_path = GetAndCreateProfileDataPath()
#     files = glob.glob(os.path.join(profile_data_path, "*.profile"))
#     
#     if len(files) == 0:
#         return 
#     
#     profile_stats = None 
#     if six.PY2:
#         profile_stats = pstats.Stats(files[0])
#         if len(files) > 1:
#             for i in range(1,len(files)):
#                 try:
#                     profile_stats.add(files[i])
#                     
#                 except EOFError:
#                     print("Could not include profile file %s" % f)
#                     pass
#     else:
#         profile_stats = pstats.Stats()
#         for f in files:
#             try:
#                 profile_stats.add(f)
#             except  EOFError:
#                 print("Could not include profile file %s" % f)
#                 pass
#         
#     profile_stats.dump_stats(output_path)
#     
#     for f in files:
#         os.remove(f)
#      

def WaitOnAllPools():
    global dictKnownPools
    for (key, pool) in list(dictKnownPools.items()):
        _sprint("Waiting on pool: " + key)
        pool.wait_completion()

@atexit.register
def ClosePools():
    '''
    Shutdown all pools.

    '''
    global dictKnownPools
    global profiler

    for (key, pool) in list(dictKnownPools.items()):
        _sprint("Waiting on pool: " + key)
        pool.shutdown()
        del pool 

    dictKnownPools.clear()


def MergeProfilerStats(root_output_dir, profile_dir, pool_name): 
    '''Called by atexit.  Merges all *.profile files in the profile_dir into a single .profile file'''
    profile_files = glob.glob(os.path.join(profile_dir, "**","*.pstats"), recursive=True)
    
    if len(profile_files) == 0:
        return
     
    agg = pstats.Stats()
    agg.add(*profile_files)
    
    output_full_path = os.path.join(root_output_dir, pool_name + '_aggregate.pstats')
    agg.dump_stats(output_full_path)
    
    #Remove the individual .profile files
    for f in profile_files:
        os.remove(f)    

if __name__ == '__main__':
    start_profiling()