from six.moves import queue
import threading
import time
import multiprocessing
import logging
import nornir_pools

class PoolBase(object):
    '''
    Pool objects provide the interface to create tasks on the pool.
    '''

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, val):
        self._name = val

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
        self._name = None

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
        
        
        self.deadthreadqueue = queue.Queue() #Threads put themselves here when they die
        self.shutdown_event = threading.Event()
        self.shutdown_event.clear()
        #self.keep_alive_thread = None
        self._threads = []
         
        self.WorkerCheckInterval=kwargs.get('WorkerCheckInterval', None)
        if self.WorkerCheckInterval is None:
            self.WorkerCheckInterval = 2
        
        self._max_threads = kwargs.get('num_threads', multiprocessing.cpu_count())
        if self._max_threads is None:
            self._max_threads = multiprocessing.cpu_count()
            
        self.tasks = queue.Queue(maxsize=self._max_threads * 2)
        
    def shutdown(self):
        if self.shutdown_event.isSet():
            return
        
        self.wait_completion()
        self.shutdown_event.set()
        
        nornir_pools._remove_pool(self)

        # Give threads time to die gracefully
        time.sleep(self.WorkerCheckInterval + 1)
        del self._threads
#         
    def add_worker_thread(self):
        raise NotImplementedError("add_worker_thread must be implemented by derived class and return a thread object")
    
    def add_threads_if_needed(self):
        
        assert(False == self.shutdown_event.isSet())
        
        self.remove_finished_threads()
        num_active_threads = len(self._threads)
        
        num_threads_created = 0
        while num_active_threads < min((self._max_threads, self.tasks.qsize()+1)):
            if not self.tasks.empty():
                t = self.add_worker_thread()
                assert(isinstance(t, threading.Thread))
                self._threads.append(t)
                num_active_threads += 1
                num_threads_created += 1
                time.sleep(0)
                
            else:
                break
            
    def remove_finished_threads(self):
        while not self.deadthreadqueue.empty():
            try:
                t = self.deadthreadqueue.get_nowait()
                if t is None:
                    break
                else:  
                    for i in range(len(self._threads)-1, 0,-1):
                        if t == self._threads[i]:
                            del self._threads[i]
                            break
            except queue.Empty as e:
                return 
                        
        return
                 

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""

        self.tasks.join() 
        self.remove_finished_threads()
                