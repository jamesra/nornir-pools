from abc import ABC, abstractmethod
from typing import Callable, Any

from nornir_pools.task import Task, TaskWithEvent

class IPool(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def num_active_tasks(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def shutdown(self):
        '''
        The pool waits for all tasks to complete and frees any resources such as threads in a thread pool
        '''
        raise NotImplementedError()

    @abstractmethod
    def wait_completion(self):
        '''
        Blocks until all tasks have completed
        '''
        raise NotImplementedError()

    @abstractmethod
    def add_task(self, name: str, func: Callable[..., Any], *args, **kwargs) -> Task:
        '''
        Call a python function on the pool

        :param str name: Friendly name of the task. Non-unique
        :param function func: Python function pointer to invoke on the pool

        :returns: task object
        :rtype: task
        '''
        raise NotImplementedError()

    @abstractmethod
    def add_process(self, name: str, func: Callable[..., Any], *args, **kwargs) -> TaskWithEvent:
        '''
        Invoke a process on the pool.  This function creates a task using name and then invokes pythons subprocess

        :param str name: Friendly name of the task. Non-unique
        :param function func: Process name to invoke using subprocess

        :returns: task object
        :rtype: task
        '''
        raise NotImplementedError()
