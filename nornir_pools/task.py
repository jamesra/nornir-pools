from abc import *
import datetime
import threading
import time
import math
from typing import Any


class Task(ABC):
    '''     
    Represents a task assigned to a pool.  Responsible for allowing the caller to wait for task completion, raising any exceptions, and returning data from the call.
    Task objects are created by adding tasks or processes to the pools.  They are not intended to be created directly by callers.
    '''
    
    _NextID = 0  # type: int
    _IDLock = threading.Lock()
     
    @property
    def task_id(self) -> int:
        return self.__task_id__
    
    @classmethod
    def GenerateID(cls) -> int:
        with cls._IDLock:
            _id = cls._NextID
            cls._NextID = cls._NextID + 1
            return _id
    
    @property
    def elapsed_time(self) -> datetime.datetime:
        endtime = self.task_end_time
        if endtime is None:
            endtime = time.time()
        
        return endtime - self.task_start_time 
    
    @property
    def elapsed_time_str(self) -> str:
        t_delta = self.elapsed_time
                        
        seconds = math.fmod(t_delta, 60)
        seconds_str = "%02.5g" % seconds
        return str(time.strftime('%H:%M:', time.gmtime(t_delta))) + seconds_str

    def __init__(self, name, *args, **kwargs):
        '''
        :param str name: friendly name of the task. Does not need to be unique
        '''
        self.__task_id__ = Task.GenerateID()
        self.args = args
        self.kwargs = kwargs
        self.name = name  # name of the task, used for debugging
        self.task_start_time = time.time()
        self.task_end_time = None
        
    def set_completion_time(self):
        if self.task_end_time is None:
            self.task_end_time = time.time()
    
    def __str__(self):
        time_position = 70
        time_str = self.elapsed_time_str
        out_string = "--- {0}".format(self.name)
        out_string += " " * (time_position - len(time_str))
        out_string += time_str
        return out_string

    @abstractmethod
    def wait(self):
        '''
        Wait for task to complete, does not return a value

        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait
        '''

        raise NotImplementedError()

    @abstractmethod
    def wait_return(self) -> Any:
        '''
        Wait for task to complete and return the value
        
        :return: The output of the task function or the stdout text of a called process
        
        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait_return
        '''
        raise Exception("Not implemented")

    @abstractmethod
    def iscompleted(self) -> bool:
        '''
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        '''
        raise NotImplementedError()
    
    def __eq__(self, other):
        if not isinstance(other, Task):
            return False
        
        return self.__task_id__ == other.__task_id__
      
    def __hash__(self):
        return self.__task_id__


class TaskWithEvent(Task, ABC):
    '''
    Task object with built-in event for completion
    '''

    def __init__(self, name, *args, **kwargs):
        super(TaskWithEvent, self).__init__(name, *args, **kwargs)
        self.completed = threading.Event()  # The event that task creators can look at to know if the task completes
        self.returncode = 0

    @property
    def iscompleted(self) -> bool:
        '''
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        '''
        return self.completed.is_set()

    def wait(self):
        self.completed.wait()
        

class SerialTask(Task):
    '''Used for debugging and profiling.  Returns a task object but the function has been run serially.'''
     
    def __init__(self, name: str, retval: Any, *args, **kwargs):
        super(SerialTask, self).__init__(name, *args, **kwargs)
        self._retval = retval
        self.returncode = 0  # type: int
        
    @property
    def iscompleted(self) -> bool:
        return True
    
    def wait(self):
        return 
    
    def wait_return(self):
        return self._retval
    
