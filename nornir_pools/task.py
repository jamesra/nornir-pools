import threading


class Task(object):
    '''     
    Represents a task assigned to a pool.  Responsible for allowing the caller to wait for task completion, raising any exceptions, and returning data from the call.
    Task objects are created by adding tasks or processes to the pools.  They are not intended to be created directly by callers.
    '''

    def __init__(self, name, *args, **kwargs):
        '''
        :param str name: friendly name of the task. Does not need to be unique
        '''
        self.args = args
        self.kwargs = kwargs
        self.name = name  # name of the task, used for debugging


    def wait(self):
        '''
        Wait for task to complete, does not return a value

        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait
        '''

        raise NotImplementedError()

    def wait_return(self):
        '''
        Wait for task to complete and return the value
        
        :return: The output of the task function or the stdout text of a called process
        
        :raises Exception: Exceptions raised during task execution are re-raised on the thread calling wait_return
        '''
        raise Exception("Not implemented")

    @property
    def iscompleted(self):
        '''
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        '''
        raise NotImplementedError()


class TaskWithEvent(Task):
    '''
    Task object with built-in event for completion
    '''

    def __init__(self, name, *args, **kwargs):
        super(TaskWithEvent, self).__init__(name, *args, **kwargs)
        self.completed = threading.Event()  # The event that task creators can look at to know if the task completes

    @property
    def iscompleted(self):
        '''
        Non-blocking test to determine if task has completed.  No exception is raised if the task raised an exception during execution until wait or wait_return is called.

        :return: True if the task is completed, otherwise False
        :rtype: bool
        '''
        return self.completed.isSet()

    def wait(self):
        self.completed.wait()