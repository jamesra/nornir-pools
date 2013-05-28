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

import Pools.task

NextGroupName = 0;

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
    JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)));
    Pools.pprint (JobQText);

class CTask(Pools.task.Task):

    def callback(self, *args, **kwargs):
        '''Function called when a remote process call returns'''

        if not args[0] is None:
            assert(isinstance(args[0], dict));
            self.__dict__.update(args[0])

        if 'error_message' in self.__dict__:
            sys.stderr.write(self.error_message)

        DecrementActiveJobCount()

        PrintJobsCount();

        self.completed.set();

    def wait_return(self):
        self.wait();

        if 'stdoutdata' in self.__dict__:
            return self.stdoutdata;
        else:
            return None;

def RemoteWorkerProcess(cmd, kwargs):

     entry = {}
     try:
        proc = subprocess.Popen(cmd, **kwargs);
        returned_value = proc.communicate(input);
        entry['returned_value'] = returned_value;
        entry['stdoutdata'] = returned_value[0].decode('utf-8');
        entry['stderrdata'] = returned_value[1].decode('utf-8');
        entry['returncode'] = proc.returncode;
        proc = None;

     except Exception as e:

        # inform operator of the name of the task throwing the exception
        # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

        error_message = "\n*** {0}\n{1}\n".format(traceback.format_exc())
        entry['error_message'] = error_message;
        sys.stderr.write(error_message)

     return entry;

class ParallelPythonProcess_Pool:

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads = None):

        self.server = pp.Server(ppservers = ("*",))

        Pools.pprint("Wait three seconds for other servers to respond")
        time.sleep(3)
        self.server.print_stats()

    def __del__(self):

        if not self.server is None:
            self.server.wait();
            self.server.print_stats()
            self.server.destroy();
            self.server = None;


    @property
    def ActiveTasks(self):
        global ActiveJobCount
        return ActiveJobCount

    def get_active_nodes(self):

        return self.server.get_active_nodes();

    def add_task(self, name, func, **kwargs):

        """Add a task to the queue, args are passed directly to subprocess.Popen"""

        global NextGroupName;

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        IncrementActiveJobCount()

        kwargs['stdout'] = subprocess.PIPE;
        kwargs['stderr'] = subprocess.PIPE;
        kwargs['shell'] = True

        NextGroupName = NextGroupName + 1;

        taskObj = CTask(name, args = None, kwargs = kwargs);
        self.server.submit(RemoteWorkerProcess, (func, kwargs), callback = taskObj.callback, globals = globals(), group = str(NextGroupName), modules = ('subprocess', 'sys'))

        PrintJobsCount();

        return taskObj;

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        self.server.print_stats()

        self.server.wait();



