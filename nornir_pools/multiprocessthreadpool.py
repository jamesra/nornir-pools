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
import time
from pathlib import Path
from typing import Callable, Dict

import nornir_pools
import nornir_pools.task
import nornir_shared.misc
from nornir_shared import prettyoutput

# from threading import Lock

_profiler = None  # type: None | cProfile.Profile
_worker_profiler_atexit_registered = False

# How long wait_completion pauses when an iteration reaped nothing: a task registered by
# add_task whose apply_async has not returned yet, or a ready-and-registered entry that
# somehow survived reaping. Small enough to be invisible next to real task durations, large
# enough that neither case can peg a core.
_WAIT_COMPLETION_POLL_SECONDS = 0.05

_SHUTDOWN_TIMEOUT_ENV = 'NORNIR_POOL_SHUTDOWN_TIMEOUT'


def _shutdown_timeout() -> float | None:
    """Seconds ``shutdown`` waits for tasks before forcing termination; ``None`` to wait forever.

    Deliberately unbounded by default. From the parent process a task whose callback will
    never fire is indistinguishable from a worker that is simply busy for a long time --
    both are just an AsyncResult that is not ready -- so any finite default would
    eventually kill legitimate long-running work, which is worse than the hang it avoids.
    Operators who would rather lose a wedged task than a wedged teardown can set this.
    """
    raw = os.environ.get(_SHUTDOWN_TIMEOUT_ENV)
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        prettyoutput.LogErr(
            f"{_SHUTDOWN_TIMEOUT_ENV} is not a number: {raw!r}. Waiting indefinitely instead.")
        return None
    if value <= 0:
        return None
    return value


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

        """Waits until the function has completed execution and returns the value returned by the function pointer

        :raises Exception: Exceptions raised during task execution are re-raised here
        """
        # get() re-raises the worker's exception, so the failure tail has to live in an
        # except block. Written as "retval = get(); if successful(): ... else: return
        # None", the else was unreachable: a failed task never reached the guard. That
        # cost the error log, which wait() does emit, and advertised a None-on-failure
        # result the code could not deliver.
        try:
            return self.asyncresult.get()
        except Exception:
            self.logger.error(
                "Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(
                    self.kwargs))
            # callbackontaskfail is invoked by get() above.
            raise

    def wait(self):

        """Wait for task to complete, does not return a value

        :raises Exception: Exceptions raised during task execution are re-raised here
        """

        self.asyncresult.wait()
        if self.asyncresult.successful():
            return

        self.logger.error(
            "Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(
                self.kwargs))
        # Raises the original exception and triggers the error callback. Nothing below
        # this line runs, so wait() and wait_return() agree: both re-raise, as the
        # abstract Task documents.
        self.asyncresult.get()

    @property
    def iscompleted(self) -> bool:
        return self.asyncresult.ready()


def _warmup_noop() -> None:
    """Picklable no-op used to spawn and prime process-pool workers."""
    return None


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
    def queued_tasks(self) -> int:
        # apply_async jobs are not separable into queue vs running from the parent.
        return 0

    @property
    def active_tasks(self) -> int:
        return len(self._active_tasks)

    @property
    def num_active_tasks(self) -> int:
        return len(self._active_tasks)

    @property
    def max_workers(self) -> int | None:
        return self._num_processes

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
        try:
            if self._tasks is not None:
                try:
                    self.wait_completion(timeout=_shutdown_timeout())
                except TimeoutError:
                    # close()/join() would hang next on the same wedged task, so skip the
                    # graceful path entirely and use the escape hatch. Teardown must not
                    # raise, so this is logged rather than propagated; wait_completion has
                    # already reported which task ids are stuck.
                    self.logger.error(
                        "Pool %s could not drain within its shutdown budget; terminating workers.",
                        self.name)
                    self.terminate_workers()
                    return

                self._tasks.close()
                self._tasks.join()

                # wait_completion only returns once _active_tasks is empty, so this
                # holds by construction and the previous bare assert could never
                # observe a leak. It is a logged check rather than an assert so that it
                # still reports under -O, names the tasks instead of failing bare, and
                # does not abort teardown if wait_completion ever gains a bounded wait.
                leaked = sorted(self._active_tasks)
                if leaked:
                    self.logger.error(
                        "Pool {0} shut down with {1} task(s) still registered as active: "
                        "{2}. Their completion callbacks never fired, so results may be "
                        "lost.".format(self.name, len(leaked), leaked))
        finally:
            # Teardown has to finish even if the calls above raise. Without this, a
            # failure left the pool in dictKnownPools wrapping a closed
            # multiprocessing.Pool, so the next lookup by name handed out a dead pool.
            self._active_tasks.clear()
            self._tasks = None
            nornir_pools._remove_pool(self)

    def warm(self, func: Callable | None = None) -> None:
        """Create workers and run *func* once per process so the first real task is not cold."""
        worker = func if func is not None else _warmup_noop
        _ = self.tasks
        for index in range(int(self._num_processes)):
            self.add_task(f"{self.name} warmup {index}", worker)

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
            # Nothing in here may raise. multiprocessing runs this from the pool's
            # _handle_results thread, and ApplyResult._set invokes it *before*
            # AsyncResult._event.set(), so an exception escaping here is catastrophic
            # rather than merely noisy. Measured on 3.14:
            #
            #   * _handle_results dies, so this task never becomes ready and any wait()
            #     on it blocks forever
            #   * every *later* task submitted to the same pool is stranded too, because
            #     no thread is left to deliver results
            #   * pool.terminate() then fails with "Cannot have cache with result_handler
            #     not alive", which takes out terminate_workers() -- the forced escape
            #     hatch this class relies on
            #
            # This used to raise ValueError when task_id was not in _active_tasks, which
            # is why wait_completion could not safely pop an entry before waiting on it.
            # Reporting instead of raising is what makes that loop fixable at all.
            try:
                if self._active_tasks.pop(task_id, None) is None:
                    self.logger.warning(
                        "Task %d was not listed in active tasks, but a result was received in pool %s. "
                        "The task was most likely already reaped by wait_completion.",
                        task_id, self.name)

                self.TryReportActiveTaskCount()
                return callback_func(result)
            except Exception:
                self.logger.exception(
                    "Completion callback for task %d in pool %s raised. Suppressed so the pool's "
                    "result handler survives; the task's own exception, if any, is still re-raised "
                    "by wait()/wait_return().", task_id, self.name)
                return None

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

        retval_task = MultiprocessThreadTask(name, None, *args, **kwargs)
        # Register before apply_async so a fast callback cannot race past _active_tasks.
        self._active_tasks[retval_task.task_id] = retval_task
        retval_task.asyncresult = self.tasks.apply_async(func, args, kwargs,  # type: ignore[attr-defined]
                                                         callback=self.callback_wrapper(retval_task.task_id,
                                                                                        retval_task.callback),
                                                         error_callback=self.callback_wrapper(retval_task.task_id,
                                                                                              retval_task.callbackontaskfail))
        if retval_task.asyncresult is None:
            del self._active_tasks[retval_task.task_id]
            raise ValueError("apply_async returned None instead of an asyncresult object")

        retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
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

    def wait_completion(self, timeout: float | None = None):

        """Wait for completion of all the tasks in the queue

        :param timeout: Seconds to wait before giving up. ``None`` waits indefinitely.
        :raises TimeoutError: If *timeout* elapses with tasks still registered. The
            message names the stuck task ids.
        :raises Exception: Exceptions raised during task execution are re-raised here,
            as before.
        """
        # This loop used to be `while self._active_tasks: for task in ...: task.wait()`,
        # with entries removed only by callback_wrapper. If a callback never fired the
        # loop could not terminate, in either of two shapes: a task whose AsyncResult
        # never completes blocked in wait() forever, or -- worse -- an entry whose result
        # had already arrived made wait() return instantly and the while re-enter, burning
        # a core indefinitely.
        #
        # The old comment here warned against popping an entry before waiting on it,
        # because callback_wrapper raised ValueError on an unknown task_id, which killed
        # the pool's result handler before it could set the AsyncResult's event. That is
        # no longer true: callback_wrapper reports instead of raising, so reaping an entry
        # ourselves is safe, and it is what guarantees this loop makes progress.
        deadline = None if timeout is None else time.monotonic() + timeout

        while self._active_tasks:
            reaped_any = False

            for task_id, task in list(self._active_tasks.items()):
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    break

                if task.asyncresult is None:
                    # add_task registers the task before apply_async returns, so a
                    # concurrent waiter can see this window. Retry rather than dereference
                    # None, which is what the previous task.wait() did here.
                    continue

                # Wait on the AsyncResult rather than task.wait() so a deadline can be
                # honoured; task.wait() below still surfaces the worker's exception.
                task.asyncresult.wait(remaining)
                if not task.asyncresult.ready():
                    continue  # Only reachable when a deadline cut the wait short.

                # ready() only becomes true after _set has finished calling the callback,
                # so if the entry is still here the callback did not remove it and never
                # will. Popping is a no-op in the normal case and the loop's only exit in
                # the stale case. The event is already set, so no waiter can be stranded.
                self._active_tasks.pop(task_id, None)
                reaped_any = True
                task.wait()

            if not self._active_tasks:
                return

            if deadline is not None and time.monotonic() >= deadline:
                stuck = sorted(self._active_tasks)
                self.logger.error(
                    "Pool %s timed out after %.1fs waiting for %d task(s) whose completion "
                    "callbacks never fired: %s. Their results are lost.",
                    self.name, timeout, len(stuck), stuck)
                raise TimeoutError(
                    "Pool {0} timed out after {1}s waiting for {2} task(s): {3}".format(
                        self.name, timeout, len(stuck), stuck))

            if not reaped_any:
                # Belt and braces. Reaping above should make every iteration either block
                # or shrink the dict, but if a future change reintroduces a
                # ready-and-registered entry this keeps it a slow wait rather than a
                # pegged core.
                time.sleep(_WAIT_COMPLETION_POLL_SECONDS)


