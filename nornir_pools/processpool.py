# threadpool.py
import math
import logging
import queue
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable

import nornir_pools
import nornir_shared.misc
from nornir_pools import poolbase
from nornir_shared import prettyoutput
from . import task


# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker
# import logging


class ImmediateProcessTask(task.TaskWithEvent):
    """Handle for a shell command run by a ProcessPool Worker thread.

    This object only carries the command and collects its outcome. Launching the
    process, decoding its output and marking completion all belong to Worker.run.
    """
    exception: Exception | None # Exception thrown by the process
    

    def __init__(self, name: str, func: str, *args, **kwargs):
        super(ImmediateProcessTask, self).__init__(name, *args, **kwargs)
        self.exception = None
        self.cmd = func  # type: str
        self.returned_value = None  # type: Any
        self.stdoutdata: str
        self.stderrdata: str
        # No Popen here. A Run() method used to launch self.cmd a second time, and a
        # _handle_proc_completion() re-decoded output Worker.run had already decoded and
        # would have raised on a failed task, where Worker.run sets returned_value to
        # None. Both were unreachable, and Run() was a standing invitation to
        # reintroduce the double-spawn its own comment warned about, so they were
        # removed rather than left as a trap. A vestigial self.proc went with them; only
        # Run() ever assigned it.

    def wait(self):
        # Wait for the pool worker to finish (sets completed + returncode/stdout).
        super(ImmediateProcessTask, self).wait()

        if self.exception is not None:
            raise self.exception
        elif self.returncode < 0:
            raise Exception("Negative return code from task but no exception detail provided")
        return

    @property
    def iscompleted(self):
        return self.completed.is_set()

    def wait_return(self) -> str:
        self.wait()
        return self.stdoutdata


class ProcessTask(task.TaskWithEvent):
    exception: Exception | None
    stdoutdata: str

    def __init__(self, name: str, func: Callable, *args, **kwargs):
        super(ProcessTask, self).__init__(name, *args, **kwargs)
        self.cmd = func
        self.exception = None
        self.stdoutdata = ""

    def wait(self):
        super(ProcessTask, self).wait()

        if self.exception is not None:
            raise self.exception
        elif self.returncode < 0:
            raise Exception("Negative return code from task but no exception detail provided")

    def wait_return(self):
        self.wait()
        return self.stdoutdata


class Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self,
                 tasks: queue.Queue,
                 deadthreadqueue: queue.Queue,
                 shutdown_event: threading.Event,
                 queue_wait_time: float,
                 pool: poolbase.LocalThreadPoolBase | None = None,
                 **kwargs):

        threading.Thread.__init__(self, **kwargs)
        self.tasks = tasks
        self.deadthreadqueue = deadthreadqueue
        self.shutdown_event = shutdown_event
        self.pool = pool
        self.daemon = True
        self.queue_wait_time = queue_wait_time
        # self.logger = logging.getLogger(__name__)
        self.start()

    def run(self):
        # print notification
        # logger = logging.getLogger(__name__ + '.Worker')

        while True:

            # Get next task from the queue (blocks thread if queue is empty until entry arrives)

            try:
                entry = self.tasks.get(True,
                                       self.queue_wait_time)  # Wait five seconds for a new entry in the queue and check if we should shutdown if nothing shows up
            except queue.Empty:
                # Check if we should kill the thread
                if self.shutdown_event.is_set():
                    # _sprint ("Queue Empty, exiting worker thread")
                    self.deadthreadqueue.put(self)
                    return
                else:
                    # logger.info("Thread #%d idle shutdown" % (self.ident))
                    self.deadthreadqueue.put(self)
                    return

            # Record start time so we get a sense of performance

            task_start_time = time.time()

            # _sprint("+++ {0}".format(entry.name))
            # logger.info("+++ {0}".format(entry.name))

            # do it!

            if self.pool is not None:
                self.pool.mark_task_started()
            try:
                if not isinstance(entry.cmd, str):
                    raise TypeError(
                        f"ProcessPool Worker expected a shell command string, got {type(entry.cmd)!r}"
                    )
                proc = subprocess.Popen(entry.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, *entry.args,
                                        **entry.kwargs)
                stdout_bytes, stderr_bytes = proc.communicate()
                entry.returned_value = (stdout_bytes, stderr_bytes)
                entry.stdoutdata = stdout_bytes.decode('utf-8') if isinstance(stdout_bytes, bytes) else stdout_bytes
                entry.stderrdata = stderr_bytes.decode('utf-8') if isinstance(stderr_bytes, bytes) else stderr_bytes
                entry.returncode = proc.returncode
                if entry.returncode != 0 and entry.exception is None:
                    entry.exception = subprocess.CalledProcessError(
                        entry.returncode, entry.cmd, output=stdout_bytes, stderr=stderr_bytes)
                entry.set_completion_time()
                proc = None

            except Exception as e:

                # inform operator of the name of the task throwing the exception
                # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

                error_message = "\n*** {0}\n{1}\n{2}\n".format(entry.name, entry.args, traceback.format_exc())
                # logger.error(error_message)
                sys.stderr.write(error_message)

                entry.exception = e

                entry.stdoutdata = None
                entry.returned_value = None
                entry.returncode = -1
                entry.stderrdata = None
                entry.set_completion_time()
            finally:
                if self.pool is not None:
                    self.pool.mark_task_finished()

            # calculate finishing time and mark task as completed

            task_end_time = time.time()
            t_delta = task_end_time - task_start_time

            # mark the object event as completed

            entry.completed.set()

            # generate the time elapsed string for output

            seconds = math.fmod(t_delta, 60)
            seconds_str = "%02.5g" % seconds
            time_str = str(time.strftime('%H:%M:', time.gmtime(t_delta))) + seconds_str

            # print the completion notice with times aligned

            time_position = 70
            out_string = "--- {0}".format(entry.name)
            out_string += " " * (time_position - len(out_string))
            out_string += time_str
            # logging.info(out_string)

            #            _sprint (out_string)
            JobsQueued = self.tasks.qsize()
            if JobsQueued > 0:
                JobQText = "Jobs Queued: " + str(self.tasks.qsize())
                JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
                nornir_pools._PrintProgressUpdate(JobQText)

            self.tasks.task_done()


class ProcessPool(poolbase.LocalThreadPoolBase):
    """Pool of threads consuming tasks from a queue"""

    def add_task(self, name: str, func: Callable, *args, **kwargs):
        return self.add_process(name, func, *args, **kwargs)

    def __init__(self, name: str, num_workers: int | None = None, WorkerCheckInterval=0.5):
        '''
        :param int num_workers: Maximum number of workers in the pool
        :param float WorkerCheckInterval: How long worker threads wait for tasks before shutting down
        '''
        super(ProcessPool, self).__init__(num_threads=num_workers, WorkerCheckInterval=WorkerCheckInterval, name=name)

        self._next_thread_id = 0
        # self.logger.warn("Creating Process Pool")

    def add_worker_thread(self) -> Worker:
        w = Worker(
            self.tasks,
            self.deadthreadqueue,
            self.shutdown_event,
            float(self.WorkerCheckInterval or 0.0),
            pool=self,
        )
        w.name = "Process pool #%d" % self._next_thread_id
        self._next_thread_id += 1
        return w

    def add_process(self, name: str, func: Callable[..., Any] | str, *args, **kwargs) -> task.TaskWithEvent:
        """
        Add a task to the queue, args are passed directly to subprocess.Popen
        :param name: The name of the task
        :param func: If a string is passed a
        process is started and the string is executed as a console command.  If a callable is passed the multiprocess
        is used to invoke the function in another process.
        """

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        if isinstance(kwargs, dict):
            if 'shell' not in kwargs:
                kwargs['shell'] = True
        else:
            kwargs = {'shell': True}

        # Ensure parent process has an established shared logging session before launching child processes.
        nornir_shared.misc.StartMultiprocessLoggingListener(level=logging.getLogger().getEffectiveLevel())

        if isinstance(func, str):
            entry = ImmediateProcessTask(name, func, *args, **kwargs)
        elif callable(func):
            raise NotImplementedError(
                "ProcessPool does not run Python callables in a child process; "
                "pass a shell command string, or use MultiprocessThreadPool.add_task for callables."
            )
        elif func is None:
            info = f"Process pool add task {name} called with 'None' as function"
            prettyoutput.LogErr(info)
            raise ValueError(info)
        else:
            info = f"Process pool add task {name} parameter was Non-callable and non-string value {func}"
            prettyoutput.LogErr(info)
            raise ValueError(info)

        self.enqueue_task(entry)
        self.add_threads_if_needed()
        self.TryReportPoolLoad()
        return entry
