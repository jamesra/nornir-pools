# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

# from threading import Lock
import socket
import subprocess
import sys
import threading
import time
import traceback
from typing import Any

import nornir_pools
from . import poolbase
from . import task

NextGroupName = 0
_JobCountLock = threading.Lock()
ActiveJobCount = 0


def IncrementActiveJobCount():
    global ActiveJobCount
    with _JobCountLock:
        ActiveJobCount += 1


def DecrementActiveJobCount():
    global ActiveJobCount
    with _JobCountLock:
        ActiveJobCount -= 1


def PrintJobsCount():
    global ActiveJobCount
    JobQText = "Jobs Queued: " + str(ActiveJobCount)
    JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
    nornir_pools._PrintProgressUpdate(JobQText)


class CTask(task.TaskWithEvent):

    #: Seconds to wait for the PP callback after ``server.wait`` returns.
    PRIMARY_CALLBACK_TIMEOUT_S = 300.0
    #: Extra seconds before failing when the primary wait expires without a callback.
    SECONDARY_CALLBACK_TIMEOUT_S = 60.0

    @property
    def server(self):
        return self._server

    @property
    def groupname(self):
        return self._groupname

    def __init__(self, server, groupname, *args, **kwargs):
        super(CTask, self).__init__(*args, **kwargs)

        self._server = server
        self._groupname = groupname
        self._callback_reached = False
        self._job_count_released = False
        self._release_lock = threading.Lock()

    def _release_job_count_once(self) -> None:
        """Decrement ActiveJobCount at most once (callback or wait timeout).

        The two callers race by design: the wait-timeout path gives up at the
        same moment a late callback may arrive on a PP thread. A bare
        check-then-set lets both observe the flag unset and double-decrement.

        This is precautionary rather than an observed failure. Under a
        GIL-enabled interpreter the unsynchronized form was not reproducibly
        wrong, but nothing guarantees the two bytecodes stay uninterleaved, and
        on a free-threaded build they genuinely can. The lock is uncontended and
        taken once per task, so correctness here is close to free.

        The decrement stays outside this lock because DecrementActiveJobCount
        takes the module-wide _JobCountLock; releasing first keeps each thread
        holding only one lock at a time.
        """
        with self._release_lock:
            if self._job_count_released:
                return
            self._job_count_released = True

        DecrementActiveJobCount()

    def callback(self, *args, **kwargs):
        '''Function called when a remote process call returns'''

        assert (len(args) > 0)
        if not args[0] is None:
            assert (isinstance(args[0], dict))
            self.__dict__.update(args[0])  # type: ignore[union-attr]

        if 'error_message' in self.__dict__:
            sys.stderr.write(self.error_message)  # type: ignore[attr-defined]

        self._release_job_count_once()

        PrintJobsCount()

        self._callback_reached = True

        self.completed.set()

    def wait(self):
        self.server.wait(self.groupname)

        # The job is done, so there is no reason for this to take more than five minutes unless an error occurred and the callback will not be reached
        self.completed.wait(self.PRIMARY_CALLBACK_TIMEOUT_S)

        if not self._callback_reached:
            nornir_pools._PrintWarning(
                "Server wait returned without a callback being called.  This usually indicates a missing package on the remote.")
            nornir_pools._PrintWarning(
                f"Waiting up to {self.SECONDARY_CALLBACK_TIMEOUT_S:.0f}s more for the callback "
                "(then fail-fast and unwind ActiveJobCount).")
            if not self.completed.wait(self.SECONDARY_CALLBACK_TIMEOUT_S):
                nornir_pools._PrintWarning(
                    "PP callback never arrived; releasing ActiveJobCount and failing the wait.")
                self._release_job_count_once()
                self.completed.set()
                raise RuntimeError(
                    "ParallelPython task callback was not reached after server.wait; "
                    "ActiveJobCount was unwound to avoid a permanent leak.")

        super(CTask, self).wait()

        # If we failed the call.  Check for an exception and raise if present
        if hasattr(self, 'exception'):
            raise self.exception  # type: ignore[attr-defined]
        elif not hasattr(self, 'returncode'):
            raise Exception("No return code from task, no exception detail provided, callback was reached")
        elif self.returncode < 0:
            raise Exception("Negative (Failure) return code from task with no exception detail provided")

    def wait_return(self):
        self.wait()

        if 'stdoutdata' in self.__dict__:
            return self.stdoutdata  # type: ignore[attr-defined]
        elif 'returned_value' in self.__dict__:
            return self.returned_value  # type: ignore[attr-defined]
        else:
            return None


def RemoteWorkerProcess(cmd, fargs):
    entry: dict[str, Any] = {}

    try:
        entry = {'type': 'RemoteWorkerProcess'}
        args = fargs[0]
        kwargs = fargs[1]

        if len(args) > 0 and len(kwargs) > 0:
            proc = subprocess.Popen(cmd, *args, **kwargs)
        elif len(args) == 0 and len(kwargs) > 0:
            proc = subprocess.Popen(cmd, **kwargs)
        elif len(args) > 0 and len(kwargs) == 0:
            proc = subprocess.Popen(cmd, *args)
        else:
            proc = subprocess.Popen(cmd)

        returned_value = proc.communicate()
        entry['returned_value'] = returned_value
        entry['stdoutdata'] = returned_value[0].decode('utf-8')  # type: ignore[union-attr]
        entry['stderrdata'] = returned_value[1].decode('utf-8')  # type: ignore[union-attr]
        entry['returncode'] = proc.returncode
        proc = None

    except Exception as e:
        # inform operator of the name of the task throwing the exception
        # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads
        entry['exception'] = e
        entry['returncode'] = -1
        entry['node'] = socket.gethostname()

        error_message = "*** {0}".format(traceback.format_exc())
        server_message = "\n*** Cluster node %s raised exception: ***\n" % socket.gethostname()
        entry['error_message'] = server_message + error_message
        # sys.stderr.write(error_message)

    return entry


def RemoteFunction(func, fargs):
    entry: dict[str, Any] = {}

    try:
        entry = {'type': 'RemoteFunction'}

        args = fargs[0]
        kwargs = fargs[1]
        if len(args) > 0 and len(kwargs) > 0:
            retval = func(*args, **kwargs)
        elif len(args) == 0 and len(kwargs) > 0:
            retval = func(**kwargs)
        elif len(args) > 0 and len(kwargs) == 0:
            retval = func(*args)
        else:
            retval = func()

        entry['returned_value'] = retval
        entry['stdoutdata'] = retval
        entry['returncode'] = 0

    except Exception as e:
        entry['returned_value'] = None
        entry['returncode'] = -1
        entry['exception'] = e
        entry['node'] = socket.gethostname()

        # inform operator of the name of the task throwing the exception
        # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

        error_message = "*** {0}".format(traceback.format_exc())
        server_message = "\n*** Cluster node %s raised exception: ***\n" % socket.gethostname()
        entry['error_message'] = server_message + error_message
        # sys.stderr.write(error_message)

    return entry


class ParallelPythonProcess_Pool(poolbase.PoolBase):
    """Pool of threads consuming tasks from a queue"""

    @property
    def server(self):
        if self._server is None:
            self._server = pp.Server(ppservers=("*",))  # type: ignore[reportUndefinedVariable]
            nornir_pools._pprint("Creating server pool, wait three seconds for other servers to respond")
            time.sleep(3)

            self._server.print_stats()

        return self._server

    def __init__(self, name: str, num_workers: int | None = None, *args, **kwargs):
        super(ParallelPythonProcess_Pool, self).__init__(name=name, *args, **kwargs)
        self._server = None

    #
    #         self.server = pp.Server(ppservers = ("*",))
    #
    #
    #         self.server.print_stats()

    #     def __del__(self):
    #
    #         if not self.server is None:
    #             self.server.wait()
    #             self.server.print_stats()
    #             self.server.destroy()
    #             self.server = None

    def shutdown(self):

        self.wait_completion()

        if not self._server is None:
            self._server.destroy()
            self._server = None

    @property
    def num_active_tasks(self) -> int:
        return ActiveJobCount

    @property
    def ActiveTasks(self) -> int:
        """Legacy alias for :attr:`num_active_tasks`.

        Retained because nornir_buildmanager.operations.tile duck-types on this
        name to throttle submission (`hasattr(pool, 'ActiveTasks')`).
        """
        return self.num_active_tasks

    def get_active_nodes(self):

        return self.server.get_active_nodes()

    def add_task(self, name, func, *args, **kwargs):
        """Add a function to be invoked on the cluster"""

        global NextGroupName

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        IncrementActiveJobCount()

        with _JobCountLock:
            group_name = NextGroupName
            NextGroupName += 1

        taskObj = CTask(self.server, group_name, name, *args, **kwargs)
        ppTask = self.server.submit(func=RemoteFunction, args=(func, (args, kwargs)), callback=taskObj.callback,
                                    globals=globals(), group=str(group_name),
                                    modules=('socket', 'traceback', 'subprocess', 'sys'))
        taskObj.ppTask = ppTask  # type: ignore[attr-defined]

        PrintJobsCount()

        return taskObj

    def add_process(self, name, func, *args, **kwargs):
        """Add a process to be invoked to the queue, args are passed directly to subprocess.Popen"""

        global NextGroupName

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        IncrementActiveJobCount()

        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
        kwargs['shell'] = True

        with _JobCountLock:
            group_name = NextGroupName
            NextGroupName += 1

        taskObj = CTask(self.server, group_name, name, *args, **kwargs)
        ppTask = self.server.submit(RemoteWorkerProcess, args=(func, (args, kwargs)), callback=taskObj.callback,
                                    globals=globals(), group=str(group_name),
                                    modules=('socket', 'traceback', 'subprocess', 'sys'))
        taskObj.ppTask = ppTask  # type: ignore[attr-defined]

        PrintJobsCount()

        return taskObj

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        if not self._server is None:
            self.server.wait()
            self.server.print_stats()
