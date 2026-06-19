# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import atexit
import cProfile
import logging
import multiprocessing
import multiprocessing.pool
import os
import sys
import tempfile
import threading
from pathlib import Path
from typing import Callable, Dict

import nornir_pools
import nornir_pools.task
# import time
import nornir_shared.misc
from nornir_shared import prettyoutput

# from threading import Lock

_profiler = None  # type: None | cProfile.Profile
_worker_profiler_atexit_registered = False


def _ensure_repo_root_on_worker_pythonpath() -> None:
    """ForkServer/spawn pool workers unpickle callables from ``tests.*``; they exec Python with ``PYTHONPATH``.

    ``pytest_configure`` / IDE may not set this before the forkserver starts, and ``sys.path`` from the parent
    is not always visible in workers. Prefer the checkout root that contains both ``nornir_pools/`` and
    ``tests/``. If ``nornir_pools`` is loaded from ``site-packages`` only, fall back to scanning ``sys.path``
    (typical monorepo ``pythonpath``).
    """
    primary = Path(__file__).resolve().parents[1]
    repo_root: Path | None = None
    if (primary / "tests").is_dir() and (primary / "nornir_pools").is_dir():
        repo_root = primary
    else:
        for entry in sys.path:
            if not entry or entry == ".":
                continue
            try:
                pe = Path(entry).resolve()
            except OSError:
                continue
            if (pe / "tests").is_dir() and (pe / "nornir_pools").is_dir():
                repo_root = pe
                break
    if repo_root is None:
        repo_root = primary
    rs = str(repo_root)
    sep = os.pathsep
    cur = os.environ.get("PYTHONPATH", "")
    parts = [p for p in cur.split(sep) if p]
    if rs not in parts:
        os.environ["PYTHONPATH"] = rs + (sep + cur if cur else "")


def _poolinit(profile_dir: str | None = None,
              initializer: Callable | None = None,
              intitializer_args: list | None = None,
              initializer_kwargs: dict | None = None):
    global _profiler
    global _worker_profiler_atexit_registered
    _profiler = None

    if profile_dir is not None:
        assert (isinstance(profile_dir, str))
        _profiler = cProfile.Profile()
        _profiler.enable()

        # One atexit per worker process; avoids stacking duplicate finalizers when workers are recycled.
        if not _worker_profiler_atexit_registered:
            atexit.register(_processfinalizer, profile_dir)
            _worker_profiler_atexit_registered = True

    if initializer is not None:
        if intitializer_args is None:
            intitializer_args = []
        if initializer_kwargs is None:
            initializer_kwargs = {}
        initializer(*intitializer_args, **initializer_kwargs)


def _processfinalizer(profile_dir: str):
    global _profiler
    if _profiler is not None:
        _profiler.disable()
        profile_filename = str.format('mp-{0}.pstats', multiprocessing.current_process().pid)
        profile_fullpath = os.path.join(profile_dir, profile_filename)
        _profiler.dump_stats(profile_fullpath)
        _profiler = None


# 
# def _pickle_method(method):
#     func_name = method.__func__.__name__
#     obj = method.__self__
#     cls = method.__self__.__class__
#     if func_name.startswith('__') and not func_name.endswith('__'):  # deal with mangled names
#         cls_name = cls.__name__.lstrip('_')
#         func_name = '_' + cls_name + func_name
#     return _unpickle_method, (func_name, obj, cls)
# 
# def _unpickle_method(func_name, obj, cls):
#     for cls in cls.__mro__:
#         try:
#             func = cls.__dict__[func_name]
#         except KeyError:
#             pass
#         else:
#             break
#     return func.__get__(obj, cls)

# copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)


class NoDaemonProcess(multiprocessing.Process):

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)  # type: ignore[assignment]


#
#     def run(self, *args, **kwargs):
#         '''
#         Method to be run in sub-process; can be overridden in sub-class
#         '''
#         global _profiler
#         
#         if _profiler is not None:
#             _profiler.enable()
#              
#         retval = super(NoDaemonProcess, self).run(*args, **kwargs) 
#         
#         if _profiler is not None:
#             _profiler.disable()
#         
#         return retval
# # #         
#     def terminate(self):
# #         '''
# #         Terminate process; sends SIGTERM signal or uses TerminateProcess()
# #         '''
#         nornir_pools.end_profiling()
#         return super(NoDaemonProcess, self).terminate() 


class NonDaemonPool(multiprocessing.pool.Pool):
    _root_profile_output_dir = None
    _instance_id = 0
    _merge_atexit_registered: set[tuple[str, str]] = set()
    _merge_atexit_lock = threading.Lock()

    @classmethod
    def _get_root_profile_output_path(cls) -> str:
        if cls._root_profile_output_dir is None:
            default_dir = tempfile.mkdtemp(prefix="nornir-pools-profile-")
            configured_path = os.environ.get("NORNIR_PROFILE")
            if configured_path:
                try:
                    resolved_path = os.path.abspath(os.path.expanduser(configured_path))
                    os.makedirs(resolved_path, exist_ok=True)
                    cls._root_profile_output_dir = resolved_path
                except (OSError, ValueError):
                    cls._root_profile_output_dir = default_dir
                    prettyoutput.Log(
                        f"NORNIR_PROFILE '{configured_path}' is invalid; using default profile path: {default_dir}")
            else:
                cls._root_profile_output_dir = default_dir

        assert cls._root_profile_output_dir is not None
        return cls._root_profile_output_dir

    def __init__(self, *args, **kwargs):
        self.profile_dir = None  # type: str | None
        self.pool_name = str.format("pool-pid_{0}_instance_{1}", multiprocessing.current_process().pid,
                                    NonDaemonPool._instance_id)

        NonDaemonPool._instance_id += 1

        # Create a directory to store profile data for each subprocess.
        # Tests may unset NORNIR_PROFILE to skip profiling I/O and atexit hooks.
        if 'NORNIR_PROFILE' in os.environ:
            root_output_dir = NonDaemonPool._get_root_profile_output_path()
            self.profile_dir = os.path.join(root_output_dir, self.pool_name)
            os.makedirs(self.profile_dir, exist_ok=True)

            merge_key = (root_output_dir, self.pool_name)
            with NonDaemonPool._merge_atexit_lock:
                if merge_key not in NonDaemonPool._merge_atexit_registered:
                    NonDaemonPool._merge_atexit_registered.add(merge_key)
                    atexit.register(nornir_pools.MergeProfilerStats, root_output_dir, self.profile_dir, self.pool_name)

            if 'initializer' in kwargs:
                # assert ('initializer' not in kwargs)
                kwargs['initargs'] = [self.profile_dir, kwargs['initializer'], kwargs['initargs']]
            else:
                kwargs['initargs'] = [self.profile_dir]

            kwargs['initializer'] = _poolinit

        super(NonDaemonPool, self).__init__(*args, **kwargs)

        # def Process(self, *args, **kwds):
    #    return NoDaemonProcess(*args, **kwds)


class MultiprocessThreadTask(nornir_pools.task.Task):

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def callback(self, result):
        pass
        # DecrementActiveJobCount()
        # PrintJobsCount()
        self.set_completion_time()
        # self.logger.info("%s" % str(self.__str__()))
        # nornir_pools._sprint("%s" % str(self.__str__()))

    def callbackontaskfail(self, result):
        """This is manually invoked by the task when a thread fails to complete"""
        # DecrementActiveJobCount()
        # PrintJobsCount()
        self.set_completion_time()

    def __init__(self, name, asyncresult, *args, **kwargs):

        super(MultiprocessThreadTask, self).__init__(name, *args, **kwargs)
        # self.args = args
        # self.kwargs = kwargs
        self.asyncresult = asyncresult

    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        retval = self.asyncresult.get()
        if self.asyncresult.successful():
            # self.logger.info("Multiprocess successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs)
            return retval
        else:
            self.logger.error(
                "Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(
                    self.kwargs))
            # self.callbackontaskfail(self) This is called by the get() function above
            return None

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.asyncresult.wait()
        if self.asyncresult.successful():
            return
        else:
            self.logger.error(
                "Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(
                    self.kwargs))
            # self.callbackontaskfail(self)
            self.asyncresult.get()  # This should cause the original exception to be raised according to multiprocess documentation and trigger the error callback as well
            return None

    @property
    def iscompleted(self) -> bool:
        return self.asyncresult.ready()


class MultiprocessThreadPool(nornir_pools.poolbase.PoolBase):
    """Pool of threads consuming tasks from a queue"""

    def add_process(self, name, func, *args, **kwargs):
        raise NotImplementedError()

    @property
    def tasks(self):
        if self._tasks is None:
            _ensure_repo_root_on_worker_pythonpath()
            log_queue = nornir_shared.misc.StartMultiprocessLoggingListener(level=logging.getLogger().getEffectiveLevel())
            self._tasks = NonDaemonPool(maxtasksperchild=self._maxtasksperchild, processes=self._num_processes,
                                        initializer=nornir_pools.init_pool_process,
                                        initargs=(log_queue, logging.getLogger().getEffectiveLevel()))

        return self._tasks

    @property
    def lock(self):
        """Parent-process ``multiprocessing.Lock`` only; not passed to workers (see init_pool_process)."""
        return self._lock

    @property
    def num_active_tasks(self) -> int:
        return len(self._active_tasks)

    def __init__(self, name: str, num_workers: int | None = None, maxtasksperchild: int | None = None,
                 authkey: bytes | None = None,
                 *args, **kwargs):
        self._tasks = None
        # Parent-only lock (not sent through Pool initializer; avoids fork/pickle surface for unused shared_lock).
        self._lock = multiprocessing.Lock()

        if num_workers is None:
            num_workers = multiprocessing.cpu_count() or 4
        num_workers = nornir_pools.ApplyOSThreadLimit(num_workers)

        self._num_processes = num_workers
        self._maxtasksperchild = maxtasksperchild
        # A list of incomplete AsyncResults
        self._active_tasks = {}  # type : Dict[int, MultiprocessThreadTask]

        # self.authkey = multiprocessing.current_process().authkey if authkey is None else authkey
        # self._shared_memory_manager = nornir_pools.get_or_create_shared_memory_manager(self.authkey)

        super(MultiprocessThreadPool, self).__init__(name=name, *args, **kwargs)

    def shutdown(self):
        if hasattr(self, 'tasks'):
            self.tasks.close()
            self.tasks.join()
            self.wait_completion()

            assert (len(self._active_tasks) == 0)
            self._tasks = None

        nornir_pools._remove_pool(self)

    def terminate_workers(self) -> None:
        """Terminate worker processes without waiting for graceful pool close (test teardown)."""
        if self._tasks is not None:
            try:
                self._tasks.terminate()
                self._tasks.join()
            except Exception:
                pass
            self._active_tasks.clear()
            self._tasks = None

    def callback_wrapper(self, task_id: int, callback_func: Callable):
        def wrapper_function(result):
            # if isinstance(retval_task, multiprocessing.pool.AsyncResult):
            #    task_id = result._nornir_task_id_
            #    if not result._nornir_task_id_ in self._active_tasks:
            #        raise ValueError("Unexpected result received")

            #    del self._active_tasks[task_id]
            if not task_id in self._active_tasks:
                raise ValueError(
                    "Task {0} not listed in active tasks, but a result was received in pool {1}...".format(task_id,
                                                                                                           str(self)))

            del self._active_tasks[task_id]
            # print("Delete task {0}".format(task_id))

            # else: Errors return an exception, which we can't easily trace back to a task
            self.TryReportActiveTaskCount()
            return callback_func(result)

        return wrapper_function

    def add_task(self, name: str, func: Callable, *args, **kwargs) -> nornir_pools.task.Task:

        """Add a task to the queue"""
        if func is None:
            prettyoutput.LogErr("Multiprocess pool add task {0} called with 'None' as function".format(name))
        if not callable(func):
            prettyoutput.LogErr(
                "Multiprocess pool add task {0} parameter was non-callable value {1} when it should be passed a function".format(
                    name, func))

        assert (callable(func))

        # I've seen an issue here were apply_async prints an exception  about not being able to import a module.  It then swallows the exception.
        # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
        # This hangs the caller if they wait on the task.

        retval_task = MultiprocessThreadTask(name, None, args, kwargs)
        retval_task.asyncresult = self.tasks.apply_async(func, args, kwargs,  # type: ignore[attr-defined]
                                                         callback=self.callback_wrapper(retval_task.task_id,
                                                                                        retval_task.callback),
                                                         error_callback=self.callback_wrapper(retval_task.task_id,
                                                                                              retval_task.callbackontaskfail))
        if retval_task.asyncresult is None:
            raise ValueError("apply_async returned None instead of an asyncresult object")

        retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
        self._active_tasks[retval_task.task_id] = retval_task
        # print("Added task #{0}".format(retval_task.task_id))

        self.TryReportActiveTaskCount()

        return retval_task

    #
    #     def starmap(self, name, func, iterable, chunksize=None):
    #
    #         """Add a task to the queue"""
    #
    #
    #         # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
    #         # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
    #         # This hangs the caller if they wait on the task.
    #
    #         retval_task = MultiprocessThreadTask(name, None)
    #         retval_task.asyncresult = self.tasks.starmap(func, iterable, chunksize=chunksize,
    #                                                          callback=self.callback_wrapper(retval_task.task_id, retval_task.callback),
    #                                                          error_callback=self.callback_wrapper(retval_task.task_id, retval_task.callbackontaskfail))
    #         if retval_task.asyncresult is None:
    #             raise ValueError("starmap_async returned None instead of an asyncresult object")
    #
    #         retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
    #         self._active_tasks[retval_task.task_id] = retval_task
    #         #print("Added task #{0}".format(retval_task.task_id))
    #
    #         return retval_task
    #
    #
    #     def starmap_async(self, name, func, iterable, chunksize=None):
    #
    #         """Add a task to the queue"""
    #
    #
    #         # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
    #         # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
    #         # This hangs the caller if they wait on the task.
    #
    #         retval_task = MultiprocessThreadTask(name, None)
    #         retval_task.asyncresult = self.tasks.starmap_async(func, iterable, chunksize=chunksize,
    #                                                          callback=self.callback_wrapper(retval_task.task_id, retval_task.callback),
    #                                                          error_callback=self.callback_wrapper(retval_task.task_id, retval_task.callbackontaskfail))
    #         if retval_task.asyncresult is None:
    #             raise ValueError("starmap_async returned None instead of an asyncresult object")
    #
    #         retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
    #         self._active_tasks[retval_task.task_id] = retval_task
    #         #print("Added task #{0}".format(retval_task.task_id))
    #
    #         return retval_task
    #

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""
        # Never pop a task out of _active_tasks before waiting: the pool's result
        # thread can deliver the outcome in that window, callback_wrapper would not
        # find task_id, raise ValueError before AsyncResult._event.set(), and wait()
        # would hang forever under multiprocess pool stress.
        while len(self._active_tasks) > 0:
            pending = list(self._active_tasks.values())
            for task in pending:
                task.wait()


