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

Pool lifecycle
--------------

It is not necessary to perform cleanup during normal scripting; :func:`ClosePools` runs
automatically at process exit via ``atexit``.  Long-running pipelines (for example
``nornir_buildmanager``) enqueue work on global pools across many stages.  **Waiting**
for tasks and **shutting down** pools are separate operations:

* **Wait** — block until queued tasks finish.  Pools stay registered and accept new work.
* **Close** — shut down workers and remove the pool from the registry (after waiting,
  unless ``skip_wait`` is used).

Thread-kind pools (``ThreadPool``, subprocess ``ProcessPool``, ``SerialPool``) run inside
the parent process.  Process-kind pools (``MultiprocessThreadPool``, ``LocalMachinePool``,
``ParallelPythonProcess_Pool``, cluster pools) keep OS worker processes alive.  Spawning
those workers is expensive, so production code keeps process pools warm across pipeline
stages and recreates thread pools at stage boundaries instead.

.. py:class:: PoolKind

   Classifies a pool for selective wait/shutdown helpers.  ``THREAD`` pools are
   in-process; ``PROCESS`` pools use separate worker processes.

.. autofunction:: WaitOnAllPools
.. autofunction:: WaitOnThreadPools
.. autofunction:: WaitOnProcessPools
.. autofunction:: CloseThreadPools
.. autofunction:: CloseProcessPools
.. autofunction:: ReleaseStagePools
.. autofunction:: ClosePools

Stage boundaries (recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

At the end of a pipeline stage that used pools (import finished, transform saved, overlay
assembly complete, ``PipelineManager.Execute`` returned, etc.), call
:func:`ReleaseStagePools`:

1. Wait for **all** thread- and process-pool tasks to finish so stage outputs are safe
   to read.
2. Shut down **thread-kind** pools so idle in-process workers do not linger.
3. Leave **process-kind** pools registered so the next stage reuses warm workers.

Use :func:`WaitOnAllPools` when you only need synchronization and will enqueue more work
immediately without releasing thread workers.  Use :func:`ClosePools` once at process
exit or test teardown when every pool must be destroyed.

Environment variables
^^^^^^^^^^^^^^^^^^^^^

``NORNIR_POOL_DIAG``
    When set to ``1``, ``true``, or ``yes``, log pool names, kinds, and active task
    counts on each lifecycle call.

``NORNIR_KEEP_PROCESS_POOLS``
    When set, :func:`CloseProcessPools` skips process-pool shutdown.  Normally
    unnecessary because :func:`ReleaseStagePools` already preserves process pools across
    stages; use only for advanced debugging or custom teardown.

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
import platform
from enum import Enum
from typing import ParamSpec, Protocol

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
from nornir_shared import misc as nornir_logging_misc
from nornir_shared import prettyoutput

__ParallelPythonAvailable = False

try:
    import nornir_pools.parallelpythonpool
except ImportError as e:
    __ParallelPythonAvailable = False
    pass

dictKnownPools = {}

max_windows_threads = 1024

shared_lock = None  # Set only when a caller passes the_lock into init_pool_process (optional; unused in-tree).

__thread_limit_warning_shown = False


# The lock can be accessed from multiprocessthreadpool from the parent process as well

def IsParallelPythonAvailable():
    return __ParallelPythonAvailable

def init_pool_process(logging_queue=None, logging_level=None, the_lock=None):
    """Worker initializer: configure queue logging. Optional ``the_lock`` sets ``shared_lock`` (legacy API)."""
    global shared_lock
    shared_lock = the_lock
    if logging_queue is not None:
        nornir_logging_misc.ConfigureWorkerQueueLogging(log_queue=logging_queue, level=logging_level)


def ApplyOSThreadLimit(num_threads: int | None) -> int | None:
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
        release = platform.release()

        if release.startswith('11'):
            return num_threads  # There is no limit on Windows 11 on Python 13 and later (May have taken effect earlier, but this is close and earlier versions are untested)
        elif num_threads > max_windows_threads:
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
_PoolFactoryParams = ParamSpec("_PoolFactoryParams")


class PoolKind(Enum):
    """Whether a pool uses in-process workers or separate OS worker processes.

    Used by selective wait/shutdown helpers.  ``THREAD`` includes vanilla
    ``ThreadPool``, subprocess ``ProcessPool``, and ``SerialPool``.  ``PROCESS``
    includes ``MultiprocessThreadPool``, ``LocalMachinePool``, and cluster backends.
    """

    THREAD = "thread"
    PROCESS = "process"


_THREAD_POOL_FACTORIES: tuple[type, ...] = (
    threadpool.ThreadPool,
    processpool.ProcessPool,
    serialpool.SerialPool,
)


def _factory_pool_kind(pool_factory: type) -> PoolKind:
    """Classify a pool factory for selective wait/shutdown helpers."""
    if pool_factory in _THREAD_POOL_FACTORIES:
        return PoolKind.THREAD
    return PoolKind.PROCESS


def _pool_kind(pool: IPool) -> PoolKind:
    """Return the pool kind recorded at creation time."""
    kind = getattr(pool, "_nornir_pool_kind", None)
    if kind is None:
        return PoolKind.THREAD
    return kind


def _pool_diag_enabled() -> bool:
    """Return True when NORNIR_POOL_DIAG requests pool lifecycle logging."""
    return os.environ.get("NORNIR_POOL_DIAG", "").strip().lower() in ("1", "true", "yes", "on")


def _keep_process_pools() -> bool:
    """Return True when process pools should stay open across pipeline stage boundaries."""
    return os.environ.get("NORNIR_KEEP_PROCESS_POOLS", "").strip().lower() in ("1", "true", "yes", "on")


def _log_pool_diag(event: str) -> None:
    """Log pool names, kinds, and active task counts when diagnostics are enabled."""
    if not _pool_diag_enabled():
        return
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: "unknown")()
    with __pool_management_lock:
        pool_summary = [
            (name, _pool_kind(pool).name, pool.num_active_tasks)
            for name, pool in dictKnownPools.items()
        ]
    logging.getLogger(__name__).info(
        "NORNIR_POOL_DIAG %s gil=%s active_threads=%s pools=%s",
        event,
        gil_enabled,
        threading.active_count(),
        pool_summary,
    )


def _snapshot_pools() -> list[tuple[str, IPool]]:
    """Return a consistent copy of the known pool map."""
    with __pool_management_lock:
        return list(dictKnownPools.items())


def _wait_pools(kind: PoolKind | None = None) -> None:
    """Block until active tasks complete on pools of the given kind (or all pools)."""
    for key, pool in _snapshot_pools():
        if kind is not None and _pool_kind(pool) != kind:
            continue
        if pool.num_active_tasks > 0:
            _sprint("Waiting on pool: {0}".format(str(pool)))
        pool.wait_completion()


def _shutdown_pools(kind: PoolKind | None = None) -> None:
    """Shut down pools of the given kind (or all pools) after waiting for tasks."""
    while True:
        pool_items = _snapshot_pools()
        targets = [
            (key, pool)
            for key, pool in pool_items
            if kind is None or _pool_kind(pool) == kind
        ]
        if not targets:
            break
        for key, pool in targets:
            live_pool: IPool | None = None
            with __pool_management_lock:
                if key in dictKnownPools:
                    live_pool = dictKnownPools[key]
            if live_pool is None:
                continue
            if live_pool.num_active_tasks > 0:
                _sprint("Waiting on pool: {0}".format(str(live_pool)))
            live_pool.shutdown()
            with __pool_management_lock:
                if key in dictKnownPools:
                    del dictKnownPools[key]


class PoolFactory(Protocol[_PoolFactoryParams]):
    def __call__(self, name: str, num_workers: int | None = None, /, *args: _PoolFactoryParams.args,
                 **kwargs: _PoolFactoryParams.kwargs) -> IPool:
        ...


def __CreatePoolFromFactory(pool_factory: PoolFactory[_PoolFactoryParams],
                            Poolname: str,
                            num_workers: int | None = None,
                            *args: _PoolFactoryParams.args, **kwargs: _PoolFactoryParams.kwargs) -> IPool:
    global dictKnownPools
    global __pool_management_lock

    with __pool_management_lock:
        if Poolname in dictKnownPools:
            pool = dictKnownPools[Poolname]
            assert (pool.__class__ == pool_factory)

            return dictKnownPools[Poolname]

        logging.info(f"Creating {Poolname} pool of type {pool_factory}")

        pool = pool_factory(Poolname, num_workers, *args, **kwargs)
        pool._nornir_pool_kind = _factory_pool_kind(pool_factory)

        dictKnownPools[Poolname] = pool

        return pool


def WaitOnAllPools() -> None:
    """Block until all known pools finish queued work without shutting them down.

    Use when later code on the same thread will enqueue more tasks and you do not
    need to release idle thread-pool workers.  Pipeline stage boundaries should
    prefer :func:`ReleaseStagePools` instead.
    """
    _log_pool_diag("WaitOnAllPools")
    _wait_pools(None)


def WaitOnThreadPools() -> None:
    """Block until thread-kind pools finish without shutting them down.

    Thread-kind pools are in-process (see :class:`PoolKind`).  This does not wait on
    multiprocessing worker pools.
    """
    _log_pool_diag("WaitOnThreadPools")
    _wait_pools(PoolKind.THREAD)


def WaitOnProcessPools() -> None:
    """Block until process-kind pools finish without shutting them down.

    Process-kind pools use OS worker processes (see :class:`PoolKind`).  Waiting does
    not destroy workers; they remain available for new tasks.
    """
    _log_pool_diag("WaitOnProcessPools")
    _wait_pools(PoolKind.PROCESS)


def CloseThreadPools(*, skip_wait: bool = False) -> None:
    """Shut down thread-kind pools after optionally waiting for their tasks.

    :param skip_wait: When ``True``, assume callers already waited (as
        :func:`ReleaseStagePools` does via :func:`WaitOnAllPools`).
    """
    _log_pool_diag("CloseThreadPools")
    if not skip_wait:
        _wait_pools(PoolKind.THREAD)
    _shutdown_pools(PoolKind.THREAD)


def CloseProcessPools() -> None:
    """Shut down process-kind pools after waiting for their tasks.

    No-op when ``NORNIR_KEEP_PROCESS_POOLS`` is set.  Production pipelines normally
    call :func:`ReleaseStagePools` at stage boundaries instead, which leaves process
    pools registered.
    """
    _log_pool_diag("CloseProcessPools")
    _wait_pools(PoolKind.PROCESS)
    if _keep_process_pools():
        return
    _shutdown_pools(PoolKind.PROCESS)


def ReleaseStagePools() -> None:
    """Synchronize a pipeline stage and release in-process pool workers.

    Waits for all thread- and process-pool tasks to complete, then shuts down
    thread-kind pools only.  Process-kind pools stay registered so later stages reuse
    warm worker processes instead of paying fork/spawn cost again.

    Call at production stage boundaries (import complete, registration transform
    saved, stos overlay assembly done, pipeline ``Execute`` finished).  Full pool
    teardown belongs in :func:`ClosePools` at process or test exit.
    """
    _log_pool_diag("ReleaseStagePools")
    WaitOnAllPools()
    CloseThreadPools(skip_wait=True)


def _terminate_process_pool_workers(pool: IPool) -> None:
    """Force-terminate multiprocessing workers when a pool supports it."""
    terminate = getattr(pool, "terminate_workers", None)
    if callable(terminate):
        terminate()
        return
    nested_mt = getattr(pool, "_mtpool", None)
    if nested_mt is not None:
        _terminate_process_pool_workers(nested_mt)
    nested_pp = getattr(pool, "_ppool", None)
    if nested_pp is not None:
        nested_pp.shutdown()


def FastClosePools() -> None:
    """Shut down all pools, force-terminating process workers when supported.

    Intended for faster test teardown when graceful multiprocessing ``join`` is too
    slow.  Production pipelines should use :func:`ReleaseStagePools` between stages
    and :func:`ClosePools` at exit instead.
    """
    _log_pool_diag("FastClosePools")
    WaitOnAllPools()
    for _, pool in _snapshot_pools():
        if _pool_kind(pool) == PoolKind.PROCESS:
            _terminate_process_pool_workers(pool)
    _shutdown_pools(None)


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
def ClosePools() -> None:
    """Shut down all known pools (wait for tasks, then destroy all workers).

    Registered as an ``atexit`` handler.  Tests and short scripts should also call
    this explicitly at teardown.  Long pipelines should use :func:`ReleaseStagePools`
    between stages and reserve this for final cleanup.
    """
    global profiler

    _log_pool_diag("ClosePools")
    _wait_pools(None)
    _shutdown_pools(None)


def GetThreadPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """
    Get or create a specific thread pool using vanilla python threads
    """
    if Poolname is None:
        return GetGlobalThreadPool()
    return __CreatePoolFromFactory(nornir_pools.threadpool.ThreadPool, Poolname, num_threads)


def GetLocalMachinePool(Poolname: str | None = None, num_threads: int | None = None, is_global=False) -> IPool:
    if Poolname is None:
        return GetGlobalLocalMachinePool()
    return __CreatePoolFromFactory(nornir_pools.local_machine_pool.LocalMachinePool, Poolname, num_threads, is_global=is_global)


def GetMultithreadingPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """Get or create a specific thread pool to execute threads in other processes on the same computer using the
    multiprocessing library """
    # warnings.warn(DeprecationWarning("GetMultithreadingPool is deprecated.  Use GetLocalMachinePool instead"))
    if Poolname is None:
        return GetGlobalMultithreadingPool()
    return __CreatePoolFromFactory(nornir_pools.multiprocessthreadpool.MultiprocessThreadPool, Poolname, num_threads)


def GetProcessPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """Get or create a specific pool to invoke shell command processes on the same computer using the subprocess
    module """
    # warnings.warn(DeprecationWarning("GetProcessPool is deprecated.  Use GetLocalMachinePool instead"))
    if Poolname is None:
        return GetGlobalProcessPool()
    return __CreatePoolFromFactory(nornir_pools.processpool.ProcessPool, Poolname, num_threads)


def GetParallelPythonPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """Get or create a specific pool to invoke functions or shell command processes on a cluster using parallel
    python """
    if Poolname is None:
        return GetGlobalClusterPool()
    return __CreatePoolFromFactory(nornir_pools.parallelpythonpool.ParallelPythonProcess_Pool, Poolname, num_threads)


def GetSerialPool(Poolname: str | None = None, num_threads: int | None = None) -> IPool:
    """
    Get or create a specific thread pool using vanilla python threads
    """
    if Poolname is None:
        raise ValueError("Must supply a pool name")
    return __CreatePoolFromFactory(nornir_pools.serialpool.SerialPool, Poolname, num_threads)


def GetGlobalSerialPool() -> IPool:
    """
    Common pool for processes on the local machine
    """
    return GetSerialPool(Poolname="Global")
    # return GetProcessPool("Global local process pool")


def GetGlobalProcessPool() -> IPool:
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
__LastConsoleWrite = datetime.datetime.now(datetime.UTC)


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

    now = datetime.datetime.now(datetime.UTC)
    delta = now - __LastConsoleWrite

    if delta.seconds < 10:
        return

    __EclipseConsoleWrite(s, newline=True)
    __LastConsoleWrite = datetime.datetime.now(datetime.UTC)


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
