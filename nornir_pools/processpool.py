# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import math
import multiprocessing
import Queue as queue
import sys
import threading
import time
import traceback
import subprocess
import logging
import os

import nornir_pools as pools

import task


class ProcessTask(task.Task):

    def __init__(self, name, func, *args, **kwargs):
        super(ProcessTask, self).__init__(name, *args, **kwargs)
        self.cmd = func

    def wait(self):
        super(ProcessTask, self).wait()

        if hasattr(self, 'exception'):
            raise self.exception
        elif self.returncode < 0:
            raise Exception("Negative return code from task but no exception detail provided")

    def wait_return(self):
        self.wait()
        return self.stdoutdata


class Worker(threading.Thread):

    """Thread executing tasks from a given tasks queue"""

    WaitTime = 0.5

    def __init__(self, tasks, shutdown_event, **kwargs):

        threading.Thread.__init__(self, **kwargs)
        self.tasks = tasks
        self.shutdown_event = shutdown_event
        self.daemon = True
        self.logger = logging.getLogger('ProcessPool')
        self.start()


    def run(self):

        while True:

            # Get next task from the queue (blocks thread if queue is empty until entry arrives)

            try:
                entry = self.tasks.get(True, Worker.WaitTime)  # Wait five seconds for a new entry in the queue and check if we should shutdown if nothing shows up
            except:
                # Check if we should kill the thread
                if(self.shutdown_event.isSet()):
                    # sprint ("Queue Empty, exiting worker thread")
                    return
                else:
                    continue


            # Record start time so we get a sense of performance

            task_start_time = time.time()

            # print notification

            # sprint("+++ {0}".format(entry.name))
            self.logger.info("+++ {0}".format(entry.name))

            # do it!

            try:

                proc = subprocess.Popen(entry.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, *entry.args, **entry.kwargs)
                entry.returned_value = proc.communicate(input)
                entry.stdoutdata = entry.returned_value[0].decode('utf-8')
                entry.stderrdata = entry.returned_value[1].decode('utf-8')
                entry.returncode = proc.returncode
                proc = None

            except Exception as e:

                # inform operator of the name of the task throwing the exception
                # also, intercept the traceback and send to stderr.write() to avoid interweaving of traceback lines from parallel threads

                error_message = "\n*** {0}\n{1}\n{2}\n".format(entry.name, entry.args, traceback.format_exc())
                self.logger.error(error_message)
                sys.stderr.write(error_message)

                entry.exception = e

                entry.stdoutdata = None
                entry.returned_value = None
                entry.returncode = -1
                entry.stderrdata = None


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
            logging.info(out_string)

#            sprint (out_string)
            JobsQueued = self.tasks.qsize()
            if JobsQueued > 0:
                JobQText = "Jobs Queued: " + str(self.tasks.qsize())
                JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
                pools.PrintProgressUpdate (JobQText)

            self.tasks.task_done()


class Process_Pool:

    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads=None):

        if (num_threads is None):
            num_threads = multiprocessing.cpu_count()

        self.shutdown_event = threading.Event()
        self.shutdown_event.clear()
        self.tasks = queue.Queue()
        self.keep_alive_thread = None

        for _ in range(int(num_threads)):
            Worker(self.tasks, self.shutdown_event)


    def Shutdown(self):
        self.wait_completion()
        self.shutdown_event.set()

        # Give threads time to die gracefully
        time.sleep(Worker.WaitTime + 1)

#     def __del__(self):
#         self.wait_completion()
#         # Close all of our threads
#         self.shutdown_event.set()
#
#         # time.sleep(Worker.WaitTime + 1)

    def __keep_alive_thread_func(self):
        self.tasks.join()

    def add_process(self, name, func, *args, **kwargs):
        """Add a task to the queue, args are passed directly to subprocess.Popen"""

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed

        start_keep_alive_thread = False
        if self.keep_alive_thread is None:
            start_keep_alive_thread = True
        elif self.keep_alive_thread.is_alive() == False:
            start_keep_alive_thread = True

        if isinstance(kwargs, dict):
            if not 'shell' in kwargs:
                kwargs['shell'] = True
        else:
            kwargs = {}
            kwargs['shell'] = True

        entry = ProcessTask(name, func, *args, **kwargs)
        self.tasks.put(entry)

        if start_keep_alive_thread:
            self.keep_alive_thread = threading.Thread(group=None, target=self.__keep_alive_thread_func, name="Thread_Pool keep alive thread")
            self.keep_alive_thread.start()

        return entry


    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""

        self.tasks.join()
