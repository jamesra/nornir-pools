import datetime
import math
import threading
import time
from abc import *
from typing import Any


class Task(ABC):
    """
    Represents a task assigned to a pool.  Responsible for allowing the caller to wait for task completion, raising any exceptions, and returning data from the call.
    Task objects are created by adding tasks or processes to the pools.  They are not intended to be created directly by callers.
    """

    __next_id: int = 0
    __task_id__: int
    __id_lock: threading.Lock = threading.Lock()
    task_start_time: float
    task_end_time: float | None = None

    name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    @property
    def task_id(self) -> int:
        """Unique ID of task"""
        return self.__task_id__

    @classmethod
    def generate_id(cls) -> int:
        """Returns the next unique ID for a task.  Thread safe."""
        with cls.__id_lock:
            _id = cls.__next_id
            cls.__next_id += 1
            return _id

    @property
    def elapsed_time(self) -> float:
        """If the task is completed, returns the time to completion.  If the task is still running,
        returns the time from the start of the task to the current time"""
        endtime = time.monotonic() if self.task_end_time is None else self.task_end_time
        return endtime - self.task_start_time

    @property
    def elapsed_time_str(self) -> str:
        """Formats the elapsed time as a string in the format HH:MM:SS.ssss"""
        t_delta = self.elapsed_time

        seconds = math.fmod(t_delta, 60)
        seconds_str = "%02.5g" % seconds
        return str(time.strftime('%H:%M:', time.gmtime(t_delta))) + seconds_str

    def __init__(self, name, *args, **kwargs):
        """
        :param str name: friendly name of the task. Does not need to be unique
        """
        self.__task_id__ = Task.generate_id()
        self.args = args
        self.kwargs = kwargs
        self.name = name  # name of the task, used for debugging
        self.task_start_time = time.monotonic()
        self.task_end_time = None

    def set_completion_time(self):
        """Marks the current time as the task completion time.  Will only set completion time on the first call."""
        if self.task_end_time is None:
            self.task_end_time = time.monotonic()

    def __str__(self):
        time_position = 70
        time_str = self.elapsed_time_str
        out_string = "--- {0}".format(self.name)
        out_string += " " * (time_position - len(time_str))
        out_string += time_str
        return out_string

    @abstractmethod
    def wait(self):
        """
        Wait for task to complete, does not return a value

        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait
        """

        raise NotImplementedError()

    @abstractmethod
    def wait_return(self) -> Any:
        """
        Wait for task to complete and return the value

        :return: The output of the task function or the stdout text of a called process

        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait_return
        """
        raise Exception("Not implemented")

    @abstractmethod
    def iscompleted(self) -> bool:
        """
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        """
        raise NotImplementedError()

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False

        return self.__task_id__ == other.__task_id__

    def __hash__(self):
        return self.__task_id__


class TaskWithEvent(Task, ABC):
    """
    Task object with built-in event for completion
    """
    returncode: int  # The return code from the process or function
    completed: threading.Event  # The event that task creators can look at to know if the task is completed

    def __init__(self, name: str, *args, **kwargs):
        super(TaskWithEvent, self).__init__(name, *args, **kwargs)
        self.completed = threading.Event()  # The event that task creators can look at to know if the task completes
        self.returncode = 0

    @property
    def iscompleted(self) -> bool:
        """
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        """
        return self.completed.is_set()

    def wait(self):
        self.completed.wait()


class SerialTask(Task):
    """Used for debugging and profiling.  Returns a task object but the function has been run serially."""

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
