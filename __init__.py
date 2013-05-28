__all__ = ['ProcessPool', 'ThreadPool', 'Task']

import ProcessPool
import Threadpool
import MultiprocessThreadpool
import ParallelPythonPool
import atexit
import os
import sys

__ProcPool = None;
__ThreadPool = None;
__MultithreadingPool = None;
__ClusterProcPool = None;

def GetGlobalProcessPool():
    global __ProcPool

    if __ProcPool is None:
        __ProcPool = ProcessPool.Process_Pool();

    return __ProcPool;

def GetGlobalClusterPool():
    global __ClusterProcPool

    if __ClusterProcPool is None:
        __ClusterProcPool = ParallelPythonPool.ParallelPythonProcess_Pool();

    return __ClusterProcPool;

def GetGlobalThreadPool():
    global __ThreadPool

    if __ThreadPool is None:
        __ThreadPool = Threadpool.Thread_Pool();

    return __ThreadPool;

def GetGlobalMultithreadingPool():
    '''Returns the shared implementation of multi-threading across processes to avoid the Python GIL'''
    global __MultithreadingPool

    if __MultithreadingPool is None:
        __MultithreadingPool = MultiprocessThreadpool.MultiprocessThread_Pool();

    return __MultithreadingPool;



def sprint(s):
    """ Thread-safe print fucntion """
    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        s = s.replace('\b', '');
        s = s.replace('.', '');
        s = s.strip();

    sys.stdout.write(s + '\n')


def pprint(s):
    """ Thread-safe print fucntion, no newline """

    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if 'ECLIPSE' in os.environ:
        s = s.replace('\b', '');
        s = s.replace('.', '');
        s = s.strip();
        s = s + '\n';

    sys.stdout.write(s);

def ClosePools():
    global __ProcPool
    global __ThreadPool
    global __MultithreadingPool
    global __ClusterProcPool
    # if not __ClusterProcPool is None:
    #    __ClusterProcPool.wait_completion();

    if not __ClusterProcPool is None:
        __ClusterProcPool.wait_completion();

    if not __ProcPool is None:
        __ProcPool.wait_completion();

    if not __ThreadPool is None:
        __ThreadPool.wait_completion();

    if not __MultithreadingPool is None:
        __MultithreadingPool.wait_completion();



    # if not __ClusterProcPool is None:
    #    del __ClusterProcPool;

    if not __ClusterProcPool is None:
        del __ClusterProcPool;

    if not __ProcPool is None:
        del __ProcPool;

    if not __ThreadPool is None:
        del __ThreadPool;

    if not __MultithreadingPool is None:
        del __MultithreadingPool;


atexit.register(ClosePools)