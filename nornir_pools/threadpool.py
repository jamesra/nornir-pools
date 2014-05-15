# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import math
import multiprocessing
from six.moves import queue
import sys
import threading
import time
import traceback
import logging

import nornir_pools.task as task

import nornir_pools as pools
import nornir_pools.poolbase as poolbase


class ThreadTask(task.TaskWithEvent):

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, val):
        self._exception = val


    def __init__(self, name, func, *args, **kwargs):

        self.func = func  # Function to be called when Task is removed from queue
        self.returned_value = None  # The value returned by the executing function
        self._exception = None

        super(ThreadTask, self).__init__(name, *args, **kwargs)

    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        self.wait()

        if not self.exception is None:
            raise self.exception

        return self.returned_value

    def wait(self):

        """Wait for task to complete, does not return a value"""

        super(ThreadTask, self).wait()

        if not self.exception is None:
            raise self.exception

        return


class Worker(threading.Thread):

    """Thread executing tasks from a given tasks queue"""

    WaitTime = 0.5

    def __init__(self, tasks, shutdown_event, **kwargs):

        threading.Thread.__init__(self, **kwargs)
        self.tasks = tasks
        self.shutdown_event = shutdown_event
        self.daemon = True
        # self.logger = logging.getLogger('ThreadPool')
        self.start()

    def run(self):

        while True:

            # Get next task from the queue (blocks thread if queue is empty until entry arrives)
            try:
                entry = self.tasks.get(True, Worker.WaitTime)  # Wait five seconds for a new entry in the queue and check if we should shutdown if nothing shows up
            except:
                # Check if we should kill the thread
                if(self.shutdown_event.isSet()):
                    # _sprint ("Queue Empty, exiting worker thread")
                    return
                else:
                    continue

            # Record start time so we get a sense of performance

            task_start_time = time.time()

            # print notification

            # self.logger.info("+++ {0}".format(entry.name))
            # _sprint("+++ {0}".format(entry.name))
            # _sprint("+")

            # do it!

            try:

                if len(entry.args) > 0 and len(entry.kwargs) > 0:
                    entry.returned_value = entry.func(*entry.args, **entry.kwargs)
                elif len(entry.args) > 0 and len(entry.kwargs) == 0:
                    entry.returned_value = entry.func(*entry.args)
                elif len(entry.args) == 0 and len(entry.kwargs) > 0:
                    entry.returned_value = entry.func(**entry.kwargs)
                else:
                    entry.returned_value = entry.func()

            except Exception as e:

                # inform operator of the name of the task throwing the exception
                # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

                entry.exception = e
                error_message = "\n*** {0}\n{1}\n".format(entry.name, traceback.format_exc())
                # self.logger.error(error_message)
                # sys.stderr.write(error_message)
                pass

            # calculate finishing time and mark task as completed

            task_end_time = time.time()
            t_delta = task_end_time - task_start_time

             # generate the time elapsed string for output

            seconds = math.fmod(t_delta, 60)
            seconds_str = "%02.5g" % seconds
            time_str = str(time.strftime('%H:%M:', time.gmtime(t_delta))) + seconds_str

            # mark the object event as completed
            entry.completed.set()

            # print the completion notice with times aligned

            time_position = 70
            out_string = "--- {0}".format(entry.name)
            out_string += " " * (time_position - len(out_string))
            out_string += time_str

            # self.logger.info(out_string)

            JobsQueued = self.tasks.qsize()
            if JobsQueued > 0:

                JobQText = "Jobs Queued: " + str(self.tasks.qsize())
                JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
                pools._PrintProgressUpdate(JobQText)

            self.tasks.task_done()


class Thread_Pool(poolbase.PoolBase):

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads=None):

        if (num_threads is None):
            num_threads = multiprocessing.cpu_count()

        self.num_threads = num_threads
        self.shutdown_event = threading.Event()
        self.logger = logging.getLogger('ThreadPool')
        self.keep_alive_thread = None
        self.tasks = queue.Queue()
        self.logger.warn("Creating Thread Pool")
        self.Threads = []
        return

    def shutdown(self):
        self.wait_completion()
        self.shutdown_event.set()
        self.Threads = []

        # Give threads time to die gracefully
        time.sleep(Worker.WaitTime + 1)
#
#     def __del__(self):
#       self.wait_completion()
#         # Close all of our threads
#         self.shutdown_event.set()
#
#         self.Threads = []
#
#         time.sleep(Worker.WaitTime + 1)

    def __keep_alive_thread_func(self):
        self.tasks.join()


    def add_task(self, name, func, *args, **kwargs):

        """Add a task to the queue"""

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        start_keep_alive_thread = False
        if self.keep_alive_thread is None:
            start_keep_alive_thread = True
        elif self.keep_alive_thread.is_alive() == False:
            start_keep_alive_thread = True

        entry = ThreadTask(name, func, *args, **kwargs)
        self.tasks.put(entry)

        # We start threadpool with no threads, and add threads as the queue grows.
        if len(self.Threads) < self.num_threads:
            JobsQueued = self.tasks.qsize()
            if JobsQueued > len(self.Threads):
                self.Threads.append(Worker(self.tasks, self.shutdown_event))

        if start_keep_alive_thread:
            self.keep_alive_thread = threading.Thread(group=None, target=self.__keep_alive_thread_func, name="Thread_Pool keep alive thread")
            self.keep_alive_thread.start()

        return entry


    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""

        self.tasks.join()
