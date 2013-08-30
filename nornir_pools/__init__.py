__all__ = ['task']

import processpool
import threadpool
import multiprocessthreadpool
import parallelpythonpool
import atexit
import os
import sys
import datetime

__ProcPool = None;
__ThreadPool = None;
__MultithreadingPool = None;
__ClusterProcPool = None;

def GetGlobalProcessPool():
    global __ProcPool

    if __ProcPool is None:
        __ProcPool = processpool.Process_Pool();

    return __ProcPool;

def GetGlobalClusterPool():
    global __ClusterProcPool

    if __ClusterProcPool is None:
        __ClusterProcPool = parallelpythonpool.ParallelPythonProcess_Pool();

    return __ClusterProcPool;

def GetGlobalThreadPool():
    global __ThreadPool

    if __ThreadPool is None:
        __ThreadPool = threadpool.Thread_Pool();

    return __ThreadPool;

def GetGlobalMultithreadingPool():
    '''Returns the shared implementation of multi-threading across processes to avoid the Python GIL'''
    global __MultithreadingPool

    if __MultithreadingPool is None:
        __MultithreadingPool = multiprocessthreadpool.MultiprocessThread_Pool();

    return __MultithreadingPool;



#ToPreventFlooding the output I only write pool size every five seconds when running under ECLIPSE
__LastConsoleWrite = datetime.datetime.utcnow()

def __EclipseConsoleWrite(s, newline=False):
    s = s.replace('\b', '');
    s = s.replace('.', '');
    s = s.strip();
    
    if newline:
        s = s + '\n'
    
    sys.stdout.write(s)
    
    
def __PrintProgressUpdateEclipse(s):
    global __LastConsoleWrite
    
    now = datetime.datetime.utcnow()
    delta = now - __LastConsoleWrite
    
    if delta.seconds < 10:
        return
    
    __EclipseConsoleWrite(s, newline=True)
    __LastConsoleWrite = datetime.datetime.utcnow()
     

def __ConsoleWrite(s, newline=False):
    if newline:
        s = s + '\n'
    
    sys.stdout.write(s)
    
    
def PrintProgressUpdate(s):
    if  'ECLIPSE' in os.environ:
        __PrintProgressUpdateEclipse(s)
        return
        
    __ConsoleWrite(s)
    

def sprint(s):
    """ Thread-safe print fucntion """
    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=True)
    else:
        __ConsoleWrite(s, newline=True)
    

def pprint(s):
    """ Thread-safe print fucntion, no newline """

    # Eclipse copies test output to the unit test window and this copy has
    # problems if the output has non-alphanumeric characters
    if  'ECLIPSE' in os.environ:
        __EclipseConsoleWrite(s, newline=False)
    else:
        __ConsoleWrite(s, newline=False)
        

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