from six.moves import queue
import threading
import time
import multiprocessing
import logging

class PoolBase(object):
    '''
    Pool objects provide the interface to create tasks on the pool.
    '''


    def shutdown(self):
        '''
        The pool waits for all tasks to complete and frees any resources such as threads in a thread pool
        '''
        raise NotImplementedError()

    def wait_completion(self):
        '''
        Blocks until all tasks have completed        
        '''
        raise NotImplementedError()

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(__name__)
    

    def add_task(self, name, func, *args, **kwargs):
        '''
        Call a python function on the pool

        :param str name: Friendly name of the task. Non-unique
        :param function func: Python function pointer to invoke on the pool

        :returns: task object
        :rtype: task
        '''
        raise NotImplementedError()

    def add_process(self, name, func, *args, **kwargs):
        '''
        Invoke a process on the pool.  This function creates a task using name and then invokes pythons subprocess

        :param str name: Friendly name of the task. Non-unique
        :param function func: Process name to invoke using subprocess

        :returns: task object
        :rtype: task
        '''
        raise NotImplementedError()
    
class LocalThreadPoolBase(PoolBase):
    '''Base class for pools that rely on local threads and a queue to dispatch jobs'''
    
    WorkerCheckInterval = 0.5 #How often workers check for new jobs in the queue
    
    def __init__(self, *args, **kwargs):
        '''
        :param int num_threads: number of threads, defaults to number of cores installed on system
        '''
        super(LocalThreadPoolBase, self).__init__(*args, **kwargs)
        
        self.tasks = queue.Queue()
        self.shutdown_event = threading.Event()
        self.shutdown_event.clear()
        self.keep_alive_thread = None
        self._threads = []
         
        self.WorkerCheckInterval=kwargs.get('WorkerCheckInterval', 0.5)
        self._max_threads = kwargs.get('num_threads', multiprocessing.cpu_count())
        if self._max_threads is None:
            self._max_threads = multiprocessing.cpu_count()
        
    def shutdown(self):
        self.wait_completion()
        self.shutdown_event.set()

        # Give threads time to die gracefully
        time.sleep(self.WorkerCheckInterval + 1)
        del self._threads
        
    def __has_keep_alive_thread(self):
        
        if self.keep_alive_thread is None:
            return False
        elif self.keep_alive_thread.is_alive() == False:
            return False
            
        return True
     
    def __keep_alive_thread_func(self):
        self.tasks.join()
    
    def __start_keep_alive_thread(self):
        self.keep_alive_thread = threading.Thread(group=None, target=self.__keep_alive_thread_func, name="Thread_Pool keep alive thread")
        self.keep_alive_thread.start()
        
    def add_worker_thread(self):
        raise NotImplementedError("add_worker_thread must be implemented by derived class and return a thread object")
    
    def add_threads_if_needed(self):
        
        self.remove_finished_threads()
        num_active_threads = len(self._threads)
        
        num_threads_created = 0
        while num_active_threads < self._max_threads:
            if not self.tasks.empty():
                t = self.add_worker_thread()
                assert(isinstance(t, threading.Thread))
                self._threads.append(t)
                num_active_threads += 1
                num_threads_created += 1
                
            else:
                break
        
        if num_threads_created > 0:
            self.logger.warn("Created %d threads for pool" % (num_threads_created))
        
        if not self.__has_keep_alive_thread():
            self.__start_keep_alive_thread()
                
    
    def remove_finished_threads(self):
        for i in range(len(self._threads)-1, 0,-1):
            t = self._threads[i]
            if not t.is_alive():
                self.logger.warn("Thread %d idle shutdown" % (t.ident))
                del self._threads[i]
                 

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""

        self.tasks.join() 
                