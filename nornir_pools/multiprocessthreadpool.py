# threadpool.py

# Initially patterned from http://code.activestate.com/recipes/577187-python-thread-pool/
# Made awesomer by James Anderson
# Made prettier by James Tucker
 
import atexit
import multiprocessing
import tempfile
import multiprocessing.pool
import logging
import nornir_pools.task

import nornir_pools

import cProfile
import os 
import time
from nornir_shared import prettyoutput

#from threading import Lock

_profiler = None

 
def _poolinit(profile_dir=None):
    
    global _profiler
    _profiler = None
     
    
    if profile_dir is not None:
        assert(isinstance(profile_dir, str))
        _profiler = cProfile.Profile()
        _profiler.enable()
        
        atexit.register(_processfinalizer, profile_dir)
    
def _processfinalizer(profile_dir):
    
    global _profiler
    if _profiler is not None:
        _profiler.disable()
        profile_filename = str.format('mp-{0}.pstats', multiprocessing.current_process().pid)
        profile_fullpath = os.path.join(profile_dir, profile_filename)
        _profiler.dump_stats(profile_fullpath)
        _profiler = None
  
# 
# def _pickle_method(method):
#     func_name = method.__func__.__name__
#     obj = method.__self__
#     cls = method.__self__.__class__
#     if func_name.startswith('__') and not func_name.endswith('__'):  # deal with mangled names
#         cls_name = cls.__name__.lstrip('_')
#         func_name = '_' + cls_name + func_name
#     return _unpickle_method, (func_name, obj, cls)
# 
# def _unpickle_method(func_name, obj, cls):
#     for cls in cls.__mro__:
#         try:
#             func = cls.__dict__[func_name]
#         except KeyError:
#             pass
#         else:
#             break
#     return func.__get__(obj, cls)

# copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)


class NoDaemonProcess(multiprocessing.Process):

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)
#          
#     def run(self, *args, **kwargs):
#         '''
#         Method to be run in sub-process; can be overridden in sub-class
#         '''
#         global _profiler
#         
#         if _profiler is not None:
#             _profiler.enable()
#              
#         retval = super(NoDaemonProcess, self).run(*args, **kwargs) 
#         
#         if _profiler is not None:
#             _profiler.disable()
#         
#         return retval
# # #         
#     def terminate(self):
# #         '''
# #         Terminate process; sends SIGTERM signal or uses TerminateProcess()
# #         '''
#         nornir_pools.end_profiling()
#         return super(NoDaemonProcess, self).terminate() 
                

class NonDaemonPool(multiprocessing.pool.Pool):
    
    _root_profile_output_dir = None
    _instance_id = 0
    
    @classmethod
    def _get_root_profile_output_path(cls):
        if cls._root_profile_output_dir is None:
            if os.path.isdir(os.environ['PROFILE']):
                cls._root_profile_output_dir = os.environ['PROFILE'] 
            else:
                cls._root_profile_output_dir = tempfile.mkdtemp()
            
        return cls._root_profile_output_dir
    
    def __init__(self,*args, **kwargs):
        self.profile_dir = None
        self.pool_name = str.format("pool-pid_{0}_instance_{1}", multiprocessing.current_process().pid, NonDaemonPool._instance_id)
        
        NonDaemonPool._instance_id = NonDaemonPool._instance_id + 1 

        #Create a directory to store profile data for each subprocess        
        if 'PROFILE' in os.environ: 
            root_output_dir = NonDaemonPool._get_root_profile_output_path()
            self.profile_dir = os.path.join(root_output_dir, self.pool_name)
                
            os.makedirs(self.profile_dir, exist_ok=True)
            
            atexit.register(nornir_pools.MergeProfilerStats, root_output_dir, self.profile_dir, self.pool_name)
            assert('initializer' not in kwargs)
            
            kwargs['initializer'] = _poolinit
            kwargs['initargs'] = [self.profile_dir]
                        
        super(NonDaemonPool, self).__init__(*args, **kwargs) 
        
    #def Process(self, *args, **kwds):
    #    return NoDaemonProcess(*args, **kwds)


class MultiprocessThreadTask(nornir_pools.task.Task):
    
    @property
    def logger(self):
        return logging.getLogger(__name__)
    
    def callback(self, result):
        pass
        #DecrementActiveJobCount()
        #PrintJobsCount()
        self.set_completion_time()
        #self.logger.info("%s" % str(self.__str__()))
        #nornir_pools._sprint("%s" % str(self.__str__()))
    
    def callbackontaskfail(self, result):
        '''This is manually invoked by the task when a thread fails to complete'''
        #DecrementActiveJobCount()
        #PrintJobsCount()
        self.set_completion_time()

    def __init__(self, name, asyncresult, *args, **kwargs):

        super(MultiprocessThreadTask, self).__init__(name, *args, **kwargs)
        #self.args = args
        #self.kwargs = kwargs
        self.asyncresult = asyncresult 

    def wait_return(self):

        """Waits until the function has completed execution and returns the value returned by the function pointer"""
        retval = self.asyncresult.get()
        if self.asyncresult.successful():
            #self.logger.info("Multiprocess successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs)
            return retval
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            #self.callbackontaskfail(self) This is called by the get() function above
            return None

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.asyncresult.wait()
        if self.asyncresult.successful():
            return
        else:
            self.logger.error("Multiprocess call not successful: " + self.name + '\nargs: ' + str(self.args) + "\nkwargs: " + str(self.kwargs))
            #self.callbackontaskfail(self)
            self.asyncresult.get()  # This should cause the original exception to be raised according to multiprocess documentation and trigger the error callback as well
            return None
        
    @property
    def iscompleted(self):
        return self.asyncresult.ready()


class MultiprocessThread_Pool(nornir_pools.poolbase.PoolBase):

    """Pool of threads consuming tasks from a queue"""

    @property
    def tasks(self):
        if self._tasks is None:
            self._tasks = NonDaemonPool(maxtasksperchild=self._maxtasksperchild, processes=self._num_processes)

        return self._tasks
    
    @property
    def num_active_tasks(self):
        return len(self._active_tasks)

    def __init__(self, num_threads=None, maxtasksperchild=None, *args, **kwargs):
        self._tasks = None
        self._num_processes = num_threads 
        self._maxtasksperchild = maxtasksperchild
        self._active_tasks = {} # A list of incomplete AsyncResults
        
        super(MultiprocessThread_Pool, self).__init__(*args, **kwargs)
    

    def shutdown(self):
        if hasattr(self, 'tasks'):
            self.tasks.close()
            self.tasks.join()
            self.wait_completion()
            
            assert(len(self._active_tasks) == 0)
            self._tasks = None

        nornir_pools._remove_pool(self)
        
    def callback_wrapper(self, task_id, callback_func):
        def wrapper_function(result):
            #if isinstance(retval_task, multiprocessing.pool.AsyncResult):
            #    task_id = result._nornir_task_id_
            #    if not result._nornir_task_id_ in self._active_tasks:
            #        raise ValueError("Unexpected result received")
                 
            #    del self._active_tasks[task_id]
            if not task_id in self._active_tasks:
                raise ValueError("Task {0} not listed in active tasks, but a result was received in pool {1}...".format(task_id, str(self)))
            
            del self._active_tasks[task_id]
            #print("Delete task {0}".format(task_id))
            
            
            #else: Errors return an exception, which we can't easily trace back to a task
            self.TryReportActiveTaskCount()
            return callback_func(result)
                
        return wrapper_function
    
    def add_task(self, name, func, *args, **kwargs):

        """Add a task to the queue"""
        if func is None:
            prettyoutput.LogErr("Multiprocess pool add task {0} called with 'None' as function".format(name))
        if callable(func) == False:
            prettyoutput.LogErr("Multiprocess pool add task {0} parameter was non-callable value {1} when it should be passed a function".format(name, func))
            
        assert(callable(func))
        
        # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
        # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
        # This hangs the caller if they wait on the task.
        
        retval_task = MultiprocessThreadTask(name, None, args, kwargs)
        retval_task.asyncresult = self.tasks.apply_async(func, args, kwargs, 
                                                         callback=self.callback_wrapper(retval_task.task_id, retval_task.callback),
                                                         error_callback=self.callback_wrapper(retval_task.task_id, retval_task.callbackontaskfail))
        if retval_task.asyncresult is None:
            raise ValueError("apply_async returned None instead of an asyncresult object")
        
        retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
        self._active_tasks[retval_task.task_id] = retval_task
        #print("Added task #{0}".format(retval_task.task_id))
        
        self.TryReportActiveTaskCount()
        
        return retval_task
#     
#     def starmap(self, name, func, iterable, chunksize=None):
# 
#         """Add a task to the queue"""
# 
#         
#         # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
#         # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
#         # This hangs the caller if they wait on the task.
#         
#         retval_task = MultiprocessThreadTask(name, None)
#         retval_task.asyncresult = self.tasks.starmap(func, iterable, chunksize=chunksize,
#                                                          callback=self.callback_wrapper(retval_task.task_id, retval_task.callback),
#                                                          error_callback=self.callback_wrapper(retval_task.task_id, retval_task.callbackontaskfail))
#         if retval_task.asyncresult is None:
#             raise ValueError("starmap_async returned None instead of an asyncresult object")
#         
#         retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
#         self._active_tasks[retval_task.task_id] = retval_task
#         #print("Added task #{0}".format(retval_task.task_id))
#         
#         return retval_task
#     
#     
#     def starmap_async(self, name, func, iterable, chunksize=None):
# 
#         """Add a task to the queue"""
# 
#         
#         # I've seen an issue here were apply_async prints an exception about not being able to import a module.  It then swallows the exception.
#         # The returned task seems valid and not complete, but the MultiprocessThreadTask's event is never set because the callback isn't used.
#         # This hangs the caller if they wait on the task.
#         
#         retval_task = MultiprocessThreadTask(name, None)
#         retval_task.asyncresult = self.tasks.starmap_async(func, iterable, chunksize=chunksize,
#                                                          callback=self.callback_wrapper(retval_task.task_id, retval_task.callback),
#                                                          error_callback=self.callback_wrapper(retval_task.task_id, retval_task.callbackontaskfail))
#         if retval_task.asyncresult is None:
#             raise ValueError("starmap_async returned None instead of an asyncresult object")
#         
#         retval_task.asyncresult._nornir_task_id_ = retval_task.task_id
#         self._active_tasks[retval_task.task_id] = retval_task
#         #print("Added task #{0}".format(retval_task.task_id))
#         
#         return retval_task
#     

    def wait_completion(self):

        """Wait for completion of all the tasks in the queue"""
        while len(self._active_tasks) > 0:
            (task_id, task) = self._active_tasks.popitem()
            self._active_tasks[task_id] = task #Re-add the item to the _active_tasks so the callback can find it and remove it as expected 
            task.wait() #use wait to ensure any exceptions are thrown

 
