# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import sys
import time
import traceback
import pp
import subprocess
from threading import Lock

import task

import nornir_pools as pools

NextGroupName = 0

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
    JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
    pools.PrintProgressUpdate(JobQText)


class CTask(task.Task):

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

    def callback(self, *args, **kwargs):
        '''Function called when a remote process call returns'''

        assert(len(args) > 0)
        if not args[0] is None:
            assert(isinstance(args[0], dict))
            self.__dict__.update(args[0])

        if 'error_message' in self.__dict__:
            sys.stderr.write(self.error_message)

        DecrementActiveJobCount()

        PrintJobsCount()

        self.completed.set()

    def wait(self):
        self.server.wait(self.groupname)

        self.completed.wait()
#        super(CTask, self).wait()

        # PP is a bit strange in that the callback only occurs if the remote process does not raise an exception
        # if not self.completed.is_set():
        #    self.callback()

        # If we failed the call.  Check for an exception and raise if present
        if hasattr(self, 'exception'):
            raise self.exception
        elif self.returncode < 0:
            raise Exception("Negative return code from task but no exception detail provided")

    def wait_return(self):
        self.wait()

        if 'stdoutdata' in self.__dict__:
            return self.stdoutdata
        else:
            return None


def RemoteWorkerProcess(cmd, args, kwargs):

    entry = {}
    try:
        proc = subprocess.Popen(cmd, *args, **kwargs)
        returned_value = proc.communicate(input)
        entry['returned_value'] = returned_value
        entry['stdoutdata'] = returned_value[0].decode('utf-8')
        entry['stderrdata'] = returned_value[1].decode('utf-8')
        entry['returncode'] = proc.returncode
        proc = None

    except Exception as e:

        # inform operator of the name of the task throwing the exception
        # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads
        entry['exception'] = e
        entry['returncode'] = -1

        error_message = "\n*** {0}\n{1}\n".format(traceback.format_exc())
        entry['error_message'] = error_message
        sys.stderr.write(error_message)
    finally:
        return entry


def RemoteFunction(func, fargs):

    entry = {}

    try:
        args = fargs[0]
        kwargs = fargs[1]
        if len(args) > 0:
            retval = func(*args, **kwargs)
        else:
            retval = func(**kwargs)
        entry['returned_value'] = retval
        entry['stdoutdata'] = retval
        entry['returncode'] = 0

    except Exception as e:
        entry['returned_value'] = None
        entry['returncode'] = -1
        entry['exception'] = e

        # inform operator of the name of the task throwing the exception
        # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

        error_message = "\n*** {0}\n{1}\n".format(traceback.format_exc())
        entry['error_message'] = error_message
        sys.stderr.write(error_message)
    finally:
        return entry


class ParallelPythonProcess_Pool:

    """Pool of threads consuming tasks from a queue"""

    @property
    def server(self):
        if self._server is None:
            self._server = pp.Server(ppservers=("*",))


            pools.pprint("Creating server pool, wait three seconds for other servers to respond")
            time.sleep(3)

            self._server.print_stats()

        return self._server

    def __init__(self, num_threads=None):

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

    def Shutdown(self):

        self.wait_completion()

        if not self._server is None:
            self._server.destroy()
            self._server = None

    @property
    def ActiveTasks(self):
        global ActiveJobCount
        return ActiveJobCount

    def get_active_nodes(self):

        return self.server.get_active_nodes()

    def add_task(self, name, func, *args, **kwargs):
        """Add a function to be invoked on the cluster"""

        global NextGroupName

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        IncrementActiveJobCount()

        taskObj = CTask(self.server, NextGroupName, name, args=args, kwargs=kwargs)
        self.server.submit(func=RemoteFunction, args=(func, (args, kwargs)), callback=taskObj.callback, globals=globals(), group=str(NextGroupName), modules=('traceback', 'subprocess', 'sys'))

        NextGroupName += 1

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

        taskObj = CTask(self.server, NextGroupName, name, args=args, kwargs=kwargs)
        self.server.submit(RemoteWorkerProcess, (func, args, kwargs), callback=taskObj.callback, globals=globals(), group=str(NextGroupName), modules=('subprocess', 'sys'))

        NextGroupName += 1

        PrintJobsCount()

        return taskObj

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        if not self._server is None:
            self.server.wait()
            self.server.print_stats()
