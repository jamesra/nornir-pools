"""

nornir_pools aims to provide a consistent interface around four different multi-threading and clustering libraries
available to Python.

The use pattern for pools is:

1. Create a pool
2. add a task or process to the pool
3. save the task object returned
4. call wait or wait_return on the task object to fetch the output or raise exceptions

Steps 3 and 4 can be skipped if output is not required.  In this case wait_completion can be called on the pool to
delay until all tasks have completed.  Note that in this pattern exceptions may be lost.

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

It is not necessary to perform any cleanup.  Functions to delete pools would not be hard to add.  ClosePools is
called automatically at script termination by atexit

.. autofunction:: nornir_pools.ClosePools

Optimization
------------

On windows there is significant overhead to passing parameters to multiprocessing jobs.  To address this I added
pickle overrides to objects being marshalled.  I also removed as many global initializations as I could from modules
loaded by the tasks.

"""

import atexit
import datetime
import glob
import logging
import os
# import cProfile
import pstats
import sys
import threading
import warnings
from typing import Callable

import nornir_pools.ipool as ipool
import nornir_pools.local_machine_pool as local_machine_pool
import nornir_pools.multiprocessthreadpool as multiprocessthreadpool
import nornir_pools.poolbase as poolbase
import nornir_pools.processpool as processpool
import nornir_pools.serialpool as serialpool
import nornir_pools.shared_memory as shared_memory
import nornir_pools.task as task
import nornir_pools.threadpool as threadpool
from nornir_pools.ipool import IPool
from nornir_pools.shared_memory import get_or_create_shared_memory_manager
from nornir_pools.task import Task
from nornir_shared import prettyoutput

__ParallelPythonAvailable = False

try:
    import nornir_pools.parallelpythonpool
except ImportError as e:
    __ParallelPythonAvailable = False
    pass

dictKnownPools = {}

max_windows_threads = 61

shared_lock = None  # A multiprocessing.Lock that all child processes shared.

__thread_limit_warning_shown = False


# The lock can be accessed from multiprocessthreadpool from the parent process as well

def init_pool_process(the_lock):
    global shared_lock
    shared_lock = the_lock


def ApplyOSThreadLimit(num_threads):
    """
    :return The minimum of the maximum number of threads on the OS, the 
    MAX_PYTHON_THREADS environment variable, or the requested num_threads
    parameter 
    """
    global max_windows_threads

    if num_threads is None:
        return None

    if 'MAX_PYTHON_THREADS' in os.environ:
        environ_max_threads = int(os.environ['MAX_PYTHON_THREADS'])
        if environ_max_threads > num_threads:
            prettyoutput.Log(
                f"Number of threads in pool limited to MAX_PYTHON_THREADS environment variable, (={num_threads} threads))")

        num_threads = min(environ_max_threads, num_threads)

    if os.name == 'nt':
        if num_threads > max_windows_threads:
            num_threads = max_windows_threads

            global __thread_limit_warning_shown
            if not __thread_limit_warning_shown:
                prettyoutput.Log(f"Number of threads in pool limited to windows handle limit of {max_windows_threads}")
                __thread_limit_warning_shown = True
            # Limit the maximum number of threads to 63 due to Windows limit
            # to waitall
            # https://stackoverflow.com/questions/65252807/multiprocessing-pool-pool-on-windows-cpu-limit-of-63

    return num_threads


__pool_management_lock = threading.RLock()


def __CreatePool(poolclass: Callable[[int, list | None, dict | None], IPool],
                 Poolname: str | None = None,
                 num_threads: int | None = None,
                 *args, **kwargs) -> IPool:
    global dictKnownPools
    global __pool_management_lock

    with __pool_management_lock:
        if Poolname is None:
            return GetGlobalLocalMachinePool()

        if Poolname in dictKnownPools:
            pool = dictKnownPools[Poolname]
            assert (pool.__class__ == poolclass)

            return dictKnownPools[Poolname]

        logging.info(f"Creating {Poolname} pool of type {poolclass}")

        pool = poolclass(num_threads, *args, **kwargs)
        pool.name = Poolname

        dictKnownPools[Poolname] = pool

        return pool


def WaitOnAllPools():
    global dictKnownPools
    global __pool_management_lock

    pool_items = None
    with __pool_management_lock:
        pool_items = list(dictKnownPools.items())

    for (key, pool) in pool_items:
        if pool.num_active_tasks > 0:
            _sprint("Waiting on pool: " + str(pool))

        pool.wait_completion()


def _remove_pool(p: str | IPool):
    """Called from pool shutdown implementations to remove the pool from the map of existing pools"""
    global dictKnownPools
    global __pool_management_lock

    pname = p
    if not isinstance(p, str):
        pname = p.name

    with __pool_management_lock:
        if pname in dictKnownPools:
            del dictKnownPools[pname]


@atexit.register
def ClosePools():
    """
    Shutdown all pools.

    """
    global dictKnownPools
    global profiler
    global __pool_management_lock

    with __pool_management_lock:
        pool_items = list(dictKnownPools.items())

    while len(pool_items) > 0:
        for (key, pool) in pool_items:
            if pool.num_active_tasks > 0:
                _sprint("Waiting on pool: {0}".format(str(pool)))

            pool = None
            with __pool_management_lock:
                if key in dictKnownPools:
                    pool = dictKnownPools[key]
                else:
                    _sprint("pool {0} no longer in known pool list.  Moving on.".format(str(pool)))

            if pool is None:
                continue

            pool.shutdown()

            with __pool_management_lock:
                if key in dictKnownPools:
                    del dictKnownPools[key]

        with __pool_management_lock:
            pool_items = list(dictKnownPools.items())


def GetThreadPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """
    Get or create a specific thread pool using vanilla python threads
    """
    return __CreatePool(nornir_pools.threadpool.ThreadPool, Poolname, num_threads)


def GetLocalMachinePool(Poolname: str | None = None, num_threads: int | None = None, is_global=False) -> IPool:
    return __CreatePool(nornir_pools.local_machine_pool.LocalMachinePool, Poolname, num_threads, is_global=is_global)


def GetMultithreadingPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """Get or create a specific thread pool to execute threads in other processes on the same computer using the
    multiprocessing library """
    # warnings.warn(DeprecationWarning("GetMultithreadingPool is deprecated.  Use GetLocalMachinePool instead"))
    return __CreatePool(nornir_pools.multiprocessthreadpool.MultiprocessThreadPool, Poolname, num_threads)


def GetProcessPool(Poolname: str | None = None, num_threads: int | None = None) -> processpool.ProcessPool:
    """Get or create a specific pool to invoke shell command processes on the same computer using the subprocess
    module """
    # warnings.warn(DeprecationWarning("GetProcessPool is deprecated.  Use GetLocalMachinePool instead"))
    return __CreatePool(nornir_pools.processpool.ProcessPool, Poolname, num_threads)


def GetParallelPythonPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """Get or create a specific pool to invoke functions or shell command processes on a cluster using parallel
    python """
    return __CreatePool(nornir_pools.parallelpythonpool.ParallelPythonProcess_Pool, Poolname, num_threads)


def GetSerialPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """
    Get or create a specific thread pool using vanilla python threads
    """
    if Poolname is None:
        raise ValueError("Must supply a pool name")
    return __CreatePool(nornir_pools.serialpool.SerialPool, Poolname, num_threads)


def GetGlobalSerialPool() -> IPool:
    """
    Common pool for processes on the local machine
    """
    return GetSerialPool(Poolname="Global")
    # return GetProcessPool("Global local process pool")


def GetGlobalProcessPool() -> processpool.ProcessPool:
    """
    Common pool for processes on the local machine
    """
    return GetProcessPool(Poolname="Global process pool")
    # return GetProcessPool("Global local process pool")


def GetGlobalLocalMachinePool() -> IPool:
    """
    Common pool for launching other processes for threads or executables.  Combines multithreading and process pool
    interface.
    """

    return GetLocalMachinePool(Poolname="Global local machine pool", is_global=True)


def GetGlobalClusterPool() -> IPool:
    """
    Get the common pool for placing tasks on the cluster
    """
    if not __ParallelPythonAvailable:
        return GetGlobalLocalMachinePool()
        # raise Exception("Parallel python is not available")

    return GetParallelPythonPool("Global cluster pool")


def GetGlobalThreadPool() -> IPool:
    """
    Common pool for thread based tasks
    """
    return GetThreadPool("Global local thread pool")


def GetGlobalMultithreadingPool() -> IPool:
    """
    Common pool for multithreading module tasks, threads run in different python processes to work around the global
    interpreter lock
    """
    # return GetGlobalLocalMachinePool()
    return GetMultithreadingPool("Global multithreading pool")


# ToPreventFlooding the output I only write pool size every five seconds when running under ECLIPSE
__LastConsoleWrite = datetime.datetime.utcnow()


def __CleanOutputForEclipse(s: str):
    s = s.replace('\b', '')
    s = s.replace('.', '')
    s = s.strip()

    return s


def __EclipseConsoleWrite(s: str, newline: bool = False):
    es = __CleanOutputForEclipse(s)
    if newline:
        es += '\n'

    sys.stdout.write(es)


def __EclipseConsoleWriteError(s: str, newline: bool = False):
    es = __CleanOutputForEclipse(s)
    if newline:
        es += '\n'

    sys.stderr.write(es)


def __PrintProgressUpdateEclipse(s: str):
    global __LastConsoleWrite

    now = datetime.datetime.utcnow()
    delta = now - __LastConsoleWrite

    if delta.seconds < 10:
        return

    __EclipseConsoleWrite(s, newline=True)
    __LastConsoleWrite = datetime.datetime.utcnow()


def __ConsoleWrite(s: str, newline: bool = False):
    if newline:
        s += '\n'

    sys.stdout.write(s)


def __ConsoleWriteError(s: str, newline: bool = False):
    if newline:
        s += '\n'

    sys.stderr.write(s)


def _PrintError(s: str):
    if 'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s)
        return

    __ConsoleWriteError(s, newline=True)


def _PrintWarning(s: str):
    if 'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return

    __ConsoleWrite(s, newline=True)


def _PrintProgressUpdate(s: str):
    if 'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return

    __ConsoleWrite(s)


def _sprint(s: str):
    """ Thread-safe print fucntion """
    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if 'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=True)
    else:
        __ConsoleWrite(s, newline=True)


def _pprint(s: str):
    """ Thread-safe print fucntion, no newline """

    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if 'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=False)
    else:
        __ConsoleWrite(s, newline=False)


profiler = None
profile_data_path = None


def GetAndCreateProfileDataPath():
    profile_data_path = os.path.join(os.getcwd(), 'pool_profiles')
    # profile_data_path = os.path.join("C:\\Temp\\Testoutput\\PoolTestBase\\", 'pool_profiles')
    os.makedirs(profile_data_path, exist_ok=True)

    return profile_data_path


def GetAndCreateProfileDataFileName():
    profile_data_path = GetAndCreateProfileDataPath()

    thread = threading.current_thread()
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


def MergeProfilerStats(root_output_dir: str, profile_dir: str, pool_name: str):
    """Called by atexit.  Merges all *.profile files in the profile_dir into a single .profile file"""
    profile_files = glob.glob(os.path.join(profile_dir, "**", "*.pstats"), recursive=True)

    if len(profile_files) == 0:
        return

    agg = pstats.Stats()
    agg.add(*profile_files)

    output_full_path = os.path.join(root_output_dir, pool_name + '_aggregate.pstats')
    agg.dump_stats(output_full_path)

    # Remove the individual .profile files
    for f in profile_files:
        os.remove(f)


if __name__ == '__main__':
    start_profiling()
