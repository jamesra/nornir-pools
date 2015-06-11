# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker

import math 
import sys
import threading
import time
import traceback
import subprocess
#import logging 

import nornir_pools as pools
from . import poolbase

from . import task


class ProcessTask(task.TaskWithEvent):

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
 
    def __init__(self, tasks, deadthreadqueue, shutdown_event, queue_wait_time, **kwargs):

        threading.Thread.__init__(self, **kwargs)
        self.tasks = tasks
        self.deadthreadqueue = deadthreadqueue
        self.shutdown_event = shutdown_event
        self.daemon = True
        self.queue_wait_time = queue_wait_time
        #self.logger = logging.getLogger(__name__)
        self.start() 

    

    def run(self):
        # print notification
        #logger = logging.getLogger(__name__ + '.Worker')
        
        while True:

            # Get next task from the queue (blocks thread if queue is empty until entry arrives)

            try:
                entry = self.tasks.get(True, self.queue_wait_time)  # Wait five seconds for a new entry in the queue and check if we should shutdown if nothing shows up
            except:
                # Check if we should kill the thread
                if(self.shutdown_event.isSet()):
                    # _sprint ("Queue Empty, exiting worker thread")
                    self.deadthreadqueue.put(self)
                    return
                else:
                    #logger.info("Thread #%d idle shutdown" % (self.ident))   
                    self.deadthreadqueue.put(self)                     
                    return
                    
            # Record start time so we get a sense of performance

            task_start_time = time.time() 

            # _sprint("+++ {0}".format(entry.name))
            #logger.info("+++ {0}".format(entry.name))

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
                #logger.error(error_message)
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
            #logging.info(out_string)

#            _sprint (out_string)
            JobsQueued = self.tasks.qsize()
            if JobsQueued > 0:
                JobQText = "Jobs Queued: " + str(self.tasks.qsize())
                JobQText = ('\b' * 40) + JobQText + (' ' * (40 - len(JobQText)))
                pools._PrintProgressUpdate (JobQText)

            self.tasks.task_done()


class Process_Pool(poolbase.LocalThreadPoolBase):

    """Pool of threads consuming tasks from a queue"""
    

    def __init__(self, num_threads=None, WorkerCheckInterval = 0.5):
        '''
        :param int num_threads: Maximum number of threads in the pool
        :param float WorkerCheckInterval: How long worker threads wait for tasks before shutting down
        '''
        super(Process_Pool, self).__init__(num_threads=num_threads, WorkerCheckInterval=WorkerCheckInterval)
        
        self._next_thread_id = 0
        #self.logger.warn("Creating Process Pool") 
        
    def add_worker_thread(self):
         
        w = Worker(self.tasks, self.deadthreadqueue, self.shutdown_event, self.WorkerCheckInterval)
        w.name = "Process pool #%d" % (self._next_thread_id)
        self._next_thread_id += 1
        return w


    def add_process(self, name, func, *args, **kwargs):
        """Add a task to the queue, args are passed directly to subprocess.Popen"""

        # keep_alive_thread is a non-daemon thread started when the queue is non-empty.
        # Python will not shut down while non-daemon threads are alive.  When the queue empties the thread exits.
        # When items are added to the queue we create a new keep_alive_thread as needed
 
        if isinstance(kwargs, dict):
            if not 'shell' in kwargs:
                kwargs['shell'] = True
        else:
            kwargs = {}
            kwargs['shell'] = True

        entry = ProcessTask(name, func, *args, **kwargs)
        self.tasks.put(entry)
        self.add_threads_if_needed()

        return entry
 
