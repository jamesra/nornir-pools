import atexit
import queue
from abc import *
import threading
import time
import multiprocessing
import logging
import nornir_pools
from nornir_pools.ipool import IPool


class PoolBase(IPool):
    '''
    Pool objects provide the interface to create tasks on the pool.
    '''

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, val: str):
        self._name = val

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

        return self._logger

    def __str__(self):
        return "Pool {0} with {1} active tasks".format(self.name, self.num_active_tasks)

    def __init__(self, *args, **kwargs):
        # self.logger = logging.getLogger(__name__)
        self._logger = None
        self._name = kwargs.get('name', None)
        self._last_job_report_time = time.time()
        self.job_report_interval_in_seconds = kwargs.get("job_report_interval", 10.0)
        self._last_num_active_tasks = 0

    def TryReportActiveTaskCount(self):
        '''
        Report the current job count if we haven't reported it recently
        '''
        if self.num_active_tasks < 2 and self._last_num_active_tasks < 2:
            return

        now = time.time()
        time_since_last_report = now - self._last_job_report_time
        if time_since_last_report > self.job_report_interval_in_seconds:
            self._last_job_report_time = now
            self._last_num_active_tasks = self.num_active_tasks
            self.PrintActiveTaskCount()

    def PrintActiveTaskCount(self):
        JobQText = "Jobs Queued: " + str(self.num_active_tasks)
        JobQText = ('\b' * 40) + JobQText + ('.' * (40 - len(JobQText)))
        nornir_pools._PrintProgressUpdate(JobQText)
        return

class LocalThreadPoolBase(PoolBase, ABC):
    '''Base class for pools that rely on local threads and a queue to dispatch jobs'''

    WorkerCheckInterval = 1  # How often workers check for events to end themselves if there are no queue events
    AtExitRegisteredWaitTime = 0 # How long we will wait atexit to ensure threads have time to shutdown.  Should be the max WorkerCheckInterval of any Threadpool started.
    AtExitLock = threading.Lock()
     
    @property
    def num_active_tasks(self) -> int:
        return self.tasks.qsize()
    
    @classmethod
    def TryRegisterAtExit(cls, wait_time: float):
        """Register a wait atexit so we don't leave threads alive when the program exits and get an error message"""
        if cls.AtExitRegisteredWaitTime >= wait_time:
            return
        
        try:
            if cls.AtExitLock.acquire():
                if cls.AtExitRegisteredWaitTime >= wait_time:
                    return
                
                atexit.register(time.sleep, wait_time)
                cls.AtExitRegisteredWaitTime = wait_time
        finally:
            cls.AtExitLock.release()

    def __init__(self, *args, **kwargs):
        '''
        :param int num_threads: number of threads, defaults to number of cores installed on system
        '''
        super(LocalThreadPoolBase, self).__init__(*args, **kwargs)
         
        self.deadthreadqueue = queue.Queue()  # Threads put themselves here when they die
        self.shutdown_event = threading.Event()
        self.shutdown_event.clear()
        # self.keep_alive_thread = None
        self._threads = []

        self.WorkerCheckInterval = kwargs.get('WorkerCheckInterval', None)
        if self.WorkerCheckInterval is None:
            self.WorkerCheckInterval = LocalThreadPoolBase.WorkerCheckInterval
            
        LocalThreadPoolBase.TryRegisterAtExit(self.WorkerCheckInterval * 1.25)

        self._max_threads = kwargs.get('num_threads', multiprocessing.cpu_count())

        if self._max_threads is None:
            self._max_threads = multiprocessing.cpu_count()

        self._max_threads = nornir_pools.ApplyOSThreadLimit(self._max_threads)

        self.tasks = queue.Queue(maxsize=self._max_threads * 32)  # Queue for tasks yet to be completed by a thread
        # self.task_exceptions = queue.Queue() #Tasks that raise an unhandled exception are added to this queue

    def shutdown(self):
        if self.shutdown_event.is_set():
            return

        self.wait_completion()
        self.shutdown_event.set()

        nornir_pools._remove_pool(self)
        
        #The static atexit method gives threads time to die gracefully 
        self._threads.clear()

    @abstractmethod
    def add_worker_thread(self):
        raise NotImplementedError("add_worker_thread must be implemented by derived class and return a thread object")

    def add_threads_if_needed(self):

        assert (self.shutdown_event.is_set() is False)

        self.remove_finished_threads()
        num_active_threads = len(self._threads)

        if num_active_threads == self._max_threads:
            return

        num_threads_needed = min(self._max_threads, self.tasks.qsize() + 1) - num_active_threads

        num_threads_created = 0
        # while num_active_threads < min((self._max_threads, self.tasks.qsize()+1)):
        while num_threads_created < num_threads_needed:
            if not self.tasks.empty():
                t = self.add_worker_thread()
                assert (isinstance(t, threading.Thread))
                self._threads.append(t)
                num_active_threads += 1
                num_threads_created += 1
                time.sleep(0)

            else:
                break

    def remove_finished_threads(self):
        try:
            while True:
                t = self.deadthreadqueue.get_nowait()
                if t is None:
                    break
                else:
                    for i in range(len(self._threads) - 1, 0, -1):
                        if t == self._threads[i]:
                            del self._threads[i]
                            break
        except queue.Empty:
            pass

        return

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue.
           Note that wait or wait_return must be called on 
           each task to detect exceptions if there were any
           """

        self.tasks.join()
        self.remove_finished_threads()
