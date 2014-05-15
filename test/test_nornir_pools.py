'''
Created on Feb 14, 2013

@author: u0490822
'''
import unittest
import os
import shutil
import nornir_pools as pools
import time
import random
import multiprocessing


def CreateFile(path, number):
    '''Create a file in the path whose name is [number].txt
       store the number in the file.'''

    filename = TestThreadPool.FilenameTemplate % number
    filenamefullpath = os.path.join(path, filename)

    with open(filenamefullpath, 'w+') as hFile:
        hFile.write(str(number))


def ReadFile(path, number):

    filename = TestThreadPool.FilenameTemplate % number
    filenamefullpath = os.path.join(path, filename)

    # test.assertTrue(os.path.exists(filenamefullpath))

    numStr = "-1"
    with open(filenamefullpath, 'r') as hFile:
        numStr = hFile.read()

    return int(numStr)

def SleepForRandomTime(MaxTime=2.0):
    sleepTime = random.random() * MaxTime
    time.sleep(sleepTime)

def CreateFileWithDelay(path, number):
        '''Create a file in the path whose name is [number].txt
           store the number in the file.'''

        SleepForRandomTime()
        return CreateFile(path, number)

def ReadFileWithDelay(path, number):

        SleepForRandomTime()
        return ReadFile(path, number)

def RaiseException(msg=None):
    if msg is None:
        msg = ""

    raise IntentionalPoolException(msg)

class IntentionalPoolException(Exception):
    pass


def VerifyExceptionBehaviour(test, pool):
    '''Ensure a pool handles an exception as expected'''
    ExceptionFound = False
    exceptText = "Intentionally Raised exception in thread test"
    try:
        task = pool.add_task(exceptText, RaiseException, exceptText)
        task.wait_return()
    except IntentionalPoolException as e:
        print("Correctly found exception in thread\n" + str(e))
        ExceptionFound = True
        pass

    test.assertTrue(ExceptionFound, "wait_return: No exception reported when raised in thread")

    try:
        task = pool.add_task(exceptText, RaiseException, exceptText)
        task.wait()
    except IntentionalPoolException as e:
        print("Correctly found exception in thread\n" + str(e))
        ExceptionFound = True
        pass

    test.assertTrue(ExceptionFound, "wait: No exception reported when raised in thread")


def SquareTheNumberWithDelay(num):
    '''Squares the number on a thread'''
    SleepForRandomTime()
    return SquareTheNumber(num)

def SquareTheNumber(num):
    '''Squares the number on a thread'''
    return num * num

def runFunctionOnPool(self, TPool, Func=None, ExpectedResults=None, numThreadsInTest=100):

    if Func is None:
        Func = SquareTheNumber

    if ExpectedResults is None:
        ExpectedResults = {}
        for i in range(0, numThreadsInTest):
            ExpectedResults[i] = SquareTheNumber(i)

    tasks = []
    for i in range(0, numThreadsInTest):
        task = TPool.add_task(str(i), Func, i)

    for task in tasks:
        retval = task.wait_return()
        self.assertEqual(ExpectedResults[task.Name], retval, "Returned value from function differs from expected value")

    return



def runFileIOOnPool(self, TPool, CreateFunc=None, ReadFunc=None, numThreadsInTest=100):

    if CreateFunc is None:
        CreateFunc = CreateFile

    if ReadFunc is None:
        ReadFunc = ReadFile

    for i in range(1, numThreadsInTest):
        TPool.add_task(str(i), CreateFunc, self.TestOutputPath, i)

    TPool.wait_completion()

    tasks = []
    for i in range(1, numThreadsInTest):
        ExpectedPath = os.path.join(self.TestOutputPath, TestThreadPool.FilenameTemplate % i)
        self.assertTrue(os.path.exists(ExpectedPath), "Missing file " + ExpectedPath)

        task = TPool.add_task(str(i), ReadFunc, self.TestOutputPath, i)
        self.assertIsNotNone(task, "Expected a task returned when calling add_task")

        tasks.append(task)

    Sum = 0
    for task in tasks:

        val = task.wait_return()
        self.assertEqual(val, int(task.name))

        Sum = Sum + val

    self.assertEqual(Sum, sum(range(1, numThreadsInTest)), "Testing to ensure each number in test range was created")

    for i in range(1, numThreadsInTest):
        filename = TestThreadPool.FilenameTemplate % i
        filenamefullpath = os.path.join(self.TestOutputPath, filename)

        task = TPool.add_task(filenamefullpath, os.remove, filenamefullpath)
        self.assertIsNotNone(task, "Expected a task returned when calling add_task")

    # Make sure if we wait for a task the task is actually done
    for task in tasks:
        val = task.wait()
        filenamefullpath = task.name

        self.assertFalse(os.path.exists(filenamefullpath), "file undeleted after task reported complete")

    self.assertEqual(0, len(os.listdir(self.TestOutputPath)))


class PoolTestBase(unittest.TestCase):

    @property
    def classname(self):
        clsstr = str(self.__class__.__name__)
        return clsstr

    def setUp(self):
        '''Create a thread pool that creates 100 files with a number in them'''

        TestBaseDir = os.getcwd()
        if 'TESTOUTPUTPATH' in os.environ:
            TestBaseDir = os.environ["TESTOUTPUTPATH"]

        self.TestOutputPath = os.path.join(TestBaseDir, self.classname)

        if os.path.exists(self.TestOutputPath):
            shutil.rmtree(self.TestOutputPath)

        os.makedirs(self.TestOutputPath)
        self.assertTrue(os.path.exists(self.TestOutputPath), "Test output directory does not exist after creation")

    def runTest(self):
        self.skipTest("PoolTestBase, no test implemented")

    def tearDown(self):
        if os.path.exists(self.TestOutputPath):
            shutil.rmtree(self.TestOutputPath)


class TestThreadPoolBase(PoolTestBase):

    FilenameTemplate = "%04d.txt"


    # def runTest(self):
    #    self.skipTest("TestThreadPoolBase, no test implemented")

class TestThreadPool(TestThreadPoolBase):

    def runTest(self):
        numThreadsInTest = 100

        CreateFile(self.TestOutputPath, 0)
        ExpectedPath = os.path.join(self.TestOutputPath, TestThreadPool.FilenameTemplate % 0)
        self.assertTrue(os.path.exists(ExpectedPath), "Function we are testing threads with does not seem to work")
        os.remove(ExpectedPath)

        # Create a 100 threads and have them create files
        TPool = pools.GetGlobalThreadPool()
        self.assertIsNotNone(TPool)

        VerifyExceptionBehaviour(self, TPool)

        runFunctionOnPool(self, TPool)
        runFileIOOnPool(self, TPool)

        TPool = pools.GetThreadPool("Test local thread pool")
        self.assertIsNotNone(TPool)
        runFileIOOnPool(self, TPool)


class TestMultiprocessThreadPool(TestThreadPoolBase):

    def runTest(self):
        numThreadsInTest = 100

        CreateFile(self.TestOutputPath, 0)
        ExpectedPath = os.path.join(self.TestOutputPath, TestThreadPool.FilenameTemplate % 0)
        self.assertTrue(os.path.exists(ExpectedPath), "Function we are testing threads with does not seem to work")
        os.remove(ExpectedPath)

        # Create a 100 threads and have them create files
        TPool = pools.GetGlobalMultithreadingPool()
        self.assertIsNotNone(TPool)

        VerifyExceptionBehaviour(self, TPool)

        runFunctionOnPool(self, TPool)
        runFileIOOnPool(self, TPool)

        TPool = pools.GetMultithreadingPool("Test multithreading pool")
        self.assertIsNotNone(TPool)
        runFileIOOnPool(self, TPool)


class TestMultiprocessThreadPoolWithRandomDelay(TestMultiprocessThreadPool):
    ''' Same as TestThreadPool, but the functions call sleep for random amounts of time'''
    FilenameTemplate = "%04d.txt"

    def runTest(self):
        TPool = pools.GetGlobalMultithreadingPool()
        self.assertIsNotNone(TPool)

        runFunctionOnPool(self, TPool, Func=SquareTheNumberWithDelay)
        runFileIOOnPool(self, TPool, CreateFunc=CreateFileWithDelay, ReadFunc=ReadFileWithDelay)


class TestThreadPoolWithRandomDelay(TestThreadPool):
    ''' Same as TestThreadPool, but the functions call sleep for random amounts of time'''
    FilenameTemplate = "%04d.txt"

    def runTest(self):
        TPool = pools.GetGlobalThreadPool()
        self.assertIsNotNone(TPool)

        runFunctionOnPool(self, TPool, Func=SquareTheNumberWithDelay)
        runFileIOOnPool(self, TPool, CreateFunc=CreateFileWithDelay, ReadFunc=ReadFileWithDelay)

class TestProcessPool(unittest.TestCase):

    def runTest(self):
        # command line parameters are different on different platforms, so  I'm keeping this simpler than the threading test for now

        PPool = pools.GetGlobalProcessPool()
        self.assertIsNotNone(PPool)

        numTasksInTest = 100
        tasks = []
        for i in range(1, numTasksInTest):
            cmd = "echo %d && exit" % i
            task = PPool.add_process(str(i), cmd, shell=True)
            tasks.append(task)

        Sum = 0
        for task in tasks:

            val = task.wait_return()
            intval = int(val)
            self.assertEqual(intval, int(task.name))

            Sum = Sum + intval

        self.assertEqual(Sum, sum(range(1, numTasksInTest)), "Testing to ensure each number in test range was created")

class TestClusterPoolFunctions(TestThreadPoolBase):

    def runTest(self):
        # command line parameters are different on different platforms, so  I'm keeping this simpler than the threading test for now

        PPool = pools.GetGlobalClusterPool()
        self.assertIsNotNone(PPool)

        VerifyExceptionBehaviour(self, PPool)

        runFunctionOnPool(self, PPool)
        runFunctionOnPool(self, PPool, Func=SquareTheNumberWithDelay)
        runFileIOOnPool(self, PPool, CreateFunc=CreateFileWithDelay, ReadFunc=ReadFileWithDelay)

        VerifyExceptionBehaviour(self, PPool)


class TestClusterPool(unittest.TestCase):

    def runTest(self):
        # command line parameters are different on different platforms, so  I'm keeping this simpler than the threading test for now

        PPool = pools.GetGlobalClusterPool()
        self.assertIsNotNone(PPool)

        numTasksInTest = 100

        nodes = PPool.get_active_nodes()
        print("Active Nodes")
        print(str(nodes))

        tasks = []
        for i in range(1, numTasksInTest):
            cmd = "echo %d && exit" % i
            task = PPool.add_process(str(i), cmd)
            tasks.append(task)

        Sum = 0
        for task in tasks:
            val = task.wait_return()
            intval = int(val)
            self.assertEqual(intval, int(task.name))

            Sum = Sum + intval

        self.assertEqual(Sum, sum(range(1, numTasksInTest)), "Testing to ensure each number in test range was created")

# class TestParallelPythonProcessPool(unittest.TestCase):
#
#    def runTest(self):
#        #command line parameters are different on different platforms, so  I'm keeping this simpler than the threading test for now
#
#        PPool = pools.GetGlobalClusterPool()
#        self.assertIsNotNone(PPool)
#
#        numTasksInTest = 100
#        tasks = []
#        for i in range(1,numTasksInTest):
#            cmd = "echo %d && exit" % i
#            PPool.add_process(str(i), cmd, shell=True)
#            tasks = i
#
#        PPool.wait_completion()
#
#        Sum = 0
#        for task in tasks:
#
#            intval = int(val)
#            self.assertEqual(intval, int(task.name))
#
#            Sum = Sum + intval
#
#        self.assertEqual(Sum, sum(range(1,numTasksInTest)), "Testing to ensure each number in test range was created")
#

if __name__ == "__main__":
    # import syssys.argv = ['', 'Test.testpools']
    multiprocessing.freeze_support()
    # unittest.main()