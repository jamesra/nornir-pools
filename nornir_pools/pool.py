class Pool(object):
    '''
    Base class for pools
    '''


    def __init__(self, params):
        '''
        Constructor
        '''

    def add_task(self, name, func, *args, **kwargs):
        '''
        Call a python function on the pool
        
        :param str name: Friendly name of the task. Non-unique
        :param function func: Python function to invoke on the Pool
        '''
        pass
