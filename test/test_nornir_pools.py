'''
Created on Feb 14, 2013

@author: u0490822
'''
import collections
import multiprocessing
import os
import random
import shutil
import time
import unittest

import nornir_pools as pools


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


def SleepForRandomTime(MaxTime=0.25):
    '''Sleep for a random amount of time and return the pid we slept on'''
    sleepTime = random.random() * MaxTime
    time.sleep(sleepTime)
    return os.getpid()


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
    ExceptionFound = False

    try:
        task = pool.add_task(exceptText, RaiseException, exceptText)
        task.wait()
    except IntentionalPoolException as e:
        print("Correctly found exception in thread\n" + str(e))
        ExceptionFound = True
        pass

    test.assertTrue(ExceptionFound, "wait: No exception reported when raised in thread")
    ExceptionFound = False

    try:
        task = pool.add_task(exceptText, RaiseException, exceptText)
        pool.wait_completion()

        task.wait()
    except IntentionalPoolException as e:
        print("Correctly found exception in thread\n" + str(e))
        ExceptionFound = True
        pass

    test.assertTrue(ExceptionFound,
                    "wait_completion: No exception reported when raised in thread and pool.wait_completion called")


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
        task.expected_result_key = i
        tasks.append(task)

    for task in tasks:
        retval = task.wait_return()
        self.assertEqual(ExpectedResults[task.expected_result_key], retval,
                         "Returned value from function differs from expected value")

    return


def runEvenDistributionOfWorkTestOnThePool(self, TPool, numTasksInTest=None):
    if not numTasksInTest:
        numTasksInTest = multiprocessing.cpu_count() * 4

    tasks = []
    for i in range(1, numTasksInTest):
        t = TPool.add_task(str(i), SleepForRandomTime)
        tasks.append(t)

    TPool.wait_completion()

    pid_collection = collections.Counter([p.wait_return() for p in tasks])
    self.assertTrue(len(pid_collection) == multiprocessing.cpu_count(), "All processes should have done work")


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

    time.sleep(0.5)  # For some reason this test fails without a short delay
    num_files = len(os.listdir(self.TestOutputPath))
    self.assertEqual(0, num_files, "Found %d files in dir %s" % (num_files, self.TestOutputPath))


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

        self.TestOutputPath = os.path.join(TestBaseDir, self.classname, self.id())

        if os.path.exists(self.TestOutputPath):
            shutil.rmtree(self.TestOutputPath)

        os.makedirs(self.TestOutputPath)
        self.assertTrue(os.path.exists(self.TestOutputPath), "Test output directory does not exist after creation")

    def runTest(self):
        self.skipTest("PoolTestBase, no test implemented")

    def tearDown(self):
        pools.ClosePools()
        pools.aggregate_profiler_data(os.path.join(self.TestOutputPath, 'aggregate_pools.profile'))
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

        # No need to test even distribution of work because the threads are all in the same process
        # runEvenDistributionOfWorkTestOnThePool(self,TPool)

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
        runEvenDistributionOfWorkTestOnThePool(self, TPool)

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
        runEvenDistributionOfWorkTestOnThePool(self, TPool)


class TestThreadPoolWithRandomDelay(TestThreadPool):
    ''' Same as TestThreadPool, but the functions call sleep for random amounts of time'''
    FilenameTemplate = "%04d.txt"

    def runTest(self):
        TPool = pools.GetGlobalThreadPool()
        self.assertIsNotNone(TPool)

        runFunctionOnPool(self, TPool, Func=SquareTheNumberWithDelay)
        runFileIOOnPool(self, TPool, CreateFunc=CreateFileWithDelay, ReadFunc=ReadFileWithDelay)

        # runEvenDistributionOfWorkTestOnThePool(self,TPool)


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

            Sum += intval

        self.assertEqual(Sum, sum(range(1, numTasksInTest)), "Testing to ensure each number in test range was created")


class TestClusterPoolFunctions(TestThreadPoolBase):

    def runTest(self):
        # command line parameters are different on different platforms, so  I'm keeping this simpler than the threading test for now

        PPool = pools.GetGlobalClusterPool()
        self.assertIsNotNone(PPool)

        VerifyExceptionBehaviour(self, PPool)
        self.assertEqual(len(PPool._mtpool._active_tasks), 0, msg="Active tasks should be empty after subtest")
        runFunctionOnPool(self, PPool)
        self.assertEqual(len(PPool._mtpool._active_tasks), 0, msg="Active tasks should be empty after subtest")
        runFunctionOnPool(self, PPool, Func=SquareTheNumberWithDelay)
        self.assertEqual(len(PPool._mtpool._active_tasks), 0, msg="Active tasks should be empty after subtest")
        runFileIOOnPool(self, PPool, CreateFunc=CreateFileWithDelay, ReadFunc=ReadFileWithDelay)
        self.assertEqual(len(PPool._mtpool._active_tasks), 0, msg="Active tasks should be empty after subtest")

        runEvenDistributionOfWorkTestOnThePool(self, PPool)

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

            Sum += intval

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
    # multiprocessing.freeze_support()
    unittest.main()
