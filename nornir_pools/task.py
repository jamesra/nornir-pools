'''

'''

import threading


class Task(object):
    '''
    Task objects are returned after adding a job to a pool. 

    :param str name: friendly name of the task. Does not need to be unique

    '''

    def __init__(self, name, *args, **kwargs):
        '''
                '''

        self.args = args
        self.kwargs = kwargs
        self.name = name  # name of the task, used for debugging
        self.completed = threading.Event()  # The event that task creators can look at to know if the task completes

    def wait(self):
        '''
        Wait for task to complete, does not return a value

        If the task raised an exception during execution the exception is re-raised on the thread calling wait
        '''

        self.completed.wait()
        return

    def wait_return(self):
        '''
        Wait for task to complete and return the value

        If the task raised an exception during execution the exception is re-raised on the thread calling wait
        '''
        raise Exception("Not implemented")

    @property
    def iscompleted(self):
        '''Non-blocking test to determine if task has completed'''
        return self.completed.isSet()
