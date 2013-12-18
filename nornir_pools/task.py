'''
Created on Feb 14, 2013

@author: u0490822
'''

import threading


class Task(object):

    def __init__(self, name, *args, **kwargs):

        self.args = args
        self.kwargs = kwargs
        self.name = name  # name of the task, used for debugging
        self.completed = threading.Event()  # The event that task creators can look at to know if the task completes

    def wait(self):

        """Wait for task to complete, does not return a value"""

        self.completed.wait()

        return

    @property
    def iscompleted(self):
        return self.completed.isSet()
