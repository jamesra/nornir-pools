import atexit
import contextlib
import logging
import multiprocessing
import queue
import threading
import time
from abc import ABC, abstractmethod

import nornir_pools
from nornir_pools.ipool import IPool


class PoolBase(IPool):
    '''
    Pool objects provide the interface to create tasks on the pool.
    '''

    @property
    def name(self) -> str:
        return self._name or ""

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

        return self._logger

    @property
    def queued_tasks(self) -> int:
        """Tasks waiting to start. Override in subclasses with a queue."""
        return 0

    @property
    def active_tasks(self) -> int:
        """Tasks currently executing. Override when in-flight is tracked."""
        return 0

    @property
    def max_workers(self) -> int | None:
        """Worker capacity when known."""
        return None

    def __str__(self):
        return "Pool {0} with {1} active tasks".format(self.name, self.num_active_tasks)

    def __init__(self, *args, **kwargs):
        # self.logger = logging.getLogger(__name__)
        self._logger = None
        self._name = kwargs.get('name', None)
        self._last_job_report_time = time.time()
        self.job_report_interval_in_seconds = kwargs.get("job_report_interval", 10.0)
        self._last_num_active_tasks = 0
        self._mqtt_report_interval_in_seconds = float(kwargs.get("mqtt_job_report_interval", 0.75))

    def TryReportActiveTaskCount(self):
        '''
        Report the current job count if we haven't reported it recently
        '''
        self.TryReportPoolLoad()

        if self.num_active_tasks < 2 and self._last_num_active_tasks < 2:
            return

        now = time.time()
        time_since_last_report = now - self._last_job_report_time
        if time_since_last_report > self.job_report_interval_in_seconds:
            self._last_job_report_time = now
            self._last_num_active_tasks = self.num_active_tasks
            self.PrintActiveTaskCount()

    def TryReportPoolLoad(self) -> None:
        """Publish dashboard ``pool_load`` for this pool (throttled in shared helper)."""
        try:
            from nornir_shared.pool_load import report_pool_load
        except ImportError:
            return
        report_pool_load(
            self.name,
            queued=self.queued_tasks,
            active=self.active_tasks,
            max_workers=self.max_workers,
            interval_s=self._mqtt_report_interval_in_seconds,
        )

    def PrintActiveTaskCount(self):
        JobQText = "Jobs Queued: " + str(self.num_active_tasks)
        JobQText = ('\b' * 40) + JobQText + ('.' * (40 - len(JobQText)))
        nornir_pools._PrintProgressUpdate(JobQText)
        return


#: Queue depth allowed per worker before an outside producer is made to wait.
_QUEUE_CAPACITY_PER_WORKER = 32

#: How often a waiting producer rechecks for queue room. Correctness does not depend on
#: being notified, so a wedged pool cannot wait forever on a missed notify.
_QUEUE_ROOM_POLL_SECONDS = 0.05

#: How long a producer waits for room before the pool says so. A full queue with no
#: workers draining it used to be a silent hang.
_QUEUE_FULL_WARN_SECONDS = 30.0

#: How often a pool repeats its nested-wait warning. The pattern is legal and sometimes
#: deliberate, so this is a fragility report, not an error, and it must not flood the log
#: when a pool is used this way throughout a run. The deadlock escalation below ignores
#: this interval.
_NESTED_WAIT_WARN_INTERVAL_SECONDS = 60.0


class LocalThreadPoolBase(PoolBase, ABC):
    '''Base class for pools that rely on local threads and a queue to dispatch jobs'''

    WorkerCheckInterval = 1  # How often workers check for events to end themselves if there are no queue events
    AtExitRegisteredWaitTime = 0  # How long we will wait atexit to ensure threads have time to shutdown.  Should be the max WorkerCheckInterval of any Threadpool started.
    AtExitLock = threading.Lock()

    @property
    def queued_tasks(self) -> int:
        return self.tasks.qsize()

    @property
    def active_tasks(self) -> int:
        with self._inflight_lock:
            return self._inflight

    @property
    def num_active_tasks(self) -> int:
        return self.queued_tasks + self.active_tasks

    @property
    def max_workers(self) -> int | None:
        return self._max_threads

    def mark_task_started(self) -> None:
        """Called by a worker when it dequeues a task to run."""
        with self._inflight_lock:
            self._inflight += 1

        # A dequeue freed a slot, so release anyone waiting for room. Checking the
        # count unlocked keeps this off the hot path when nobody is waiting; a missed
        # notify costs one poll interval because the wait below is timed.
        if self._producers_waiting > 0:
            with self._queue_room:
                self._queue_room.notify_all()

        self.TryReportPoolLoad()

    def mark_task_finished(self) -> None:
        """Called by a worker when a dequeued task finishes."""
        with self._inflight_lock:
            if self._inflight > 0:
                self._inflight -= 1
        self.TryReportPoolLoad()

    @classmethod
    def TryRegisterAtExit(cls, wait_time: float):
        """Register a wait atexit so we don't leave threads alive when the program exits and get an error message"""
        if cls.AtExitRegisteredWaitTime >= wait_time:
            return

        try:
            if cls.AtExitLock.acquire():
                if cls.AtExitRegisteredWaitTime >= wait_time:
                    return

                atexit.register(time.sleep, wait_time)
                cls.AtExitRegisteredWaitTime = wait_time
        finally:
            cls.AtExitLock.release()

    def __init__(self, *args, **kwargs):
        '''
        :param int num_threads: number of threads, defaults to number of cores installed on system
        '''
        super(LocalThreadPoolBase, self).__init__(*args, **kwargs)

        self.deadthreadqueue = queue.Queue()  # Threads put themselves here when they die
        self.shutdown_event = threading.Event()
        self.shutdown_event.clear()
        # self.keep_alive_thread = None
        self._threads = []
        self._inflight = 0
        self._inflight_lock = threading.Lock()

        self.WorkerCheckInterval = kwargs.get('WorkerCheckInterval', None)
        if self.WorkerCheckInterval is None:
            self.WorkerCheckInterval = LocalThreadPoolBase.WorkerCheckInterval

        LocalThreadPoolBase.TryRegisterAtExit((self.WorkerCheckInterval or 1) * 1.25)

        self._max_threads = kwargs.get('num_threads', multiprocessing.cpu_count())

        if self._max_threads is None:
            self._max_threads = multiprocessing.cpu_count() or 1

        self._max_threads = nornir_pools.ApplyOSThreadLimit(self._max_threads)

        # The queue itself is unbounded; capacity is applied in enqueue_task, and only
        # to producers outside the pool. A bounded queue with a blocking put deadlocks
        # unrecoverably when the producer is one of this pool's own workers: it blocks
        # holding the very thread needed to drain the queue, and once every worker is
        # blocked that way nothing can ever drain it.
        self.tasks = queue.Queue()  # Queue for tasks yet to be completed by a thread
        self._queue_capacity = (self._max_threads or 1) * _QUEUE_CAPACITY_PER_WORKER
        self._queue_room = threading.Condition()
        self._producers_waiting = 0
        self._nested_waiters = 0
        self._nested_wait_lock = threading.Lock()
        self._last_nested_wait_warning = 0.0
        # self.task_exceptions = queue.Queue() #Tasks that raise an unhandled exception are added to this queue

    def called_from_pool_worker(self) -> bool:
        """True when the calling thread is one of this pool's own workers.

        Worker threads record the pool they serve, so this is exact rather than a guess
        based on thread names or identity reuse.
        """
        return getattr(threading.current_thread(), 'pool', None) is self

    @contextlib.contextmanager
    def nested_wait_guard(self, task_name: str):
        """Report a worker of this pool blocking on a task belonging to this same pool.

        #85 removed the queue-capacity half of this hazard, so a worker submitting onto
        its own pool is no longer throttled. Submitting is only half the pattern: a worker
        that then *waits* for its child consumes the very worker the child needs, because
        the child is queued behind the parent that is still occupying its thread. With
        every worker doing this the pool cannot drain and is wedged permanently.

        Nothing here changes that. Work-stealing or growing the pool would, but both are
        material behavioural changes -- re-entrant execution on a thread that is mid-task
        can surprise anything holding a lock, and growing breaks the ``num_threads``
        contract that ``add_threads_if_needed`` accounts against. This makes the fragility
        visible instead: a warning while it is merely wasteful, and an error once it is
        provably a deadlock, naming the pool so the report points at the fix.

        A no-op unless the caller really is one of this pool's workers, so the ordinary
        outside-caller wait pays only one attribute lookup.
        """
        if not self.called_from_pool_worker():
            yield
            return

        with self._nested_wait_lock:
            self._nested_waiters += 1
            waiters = self._nested_waiters

        try:
            self._report_nested_wait(task_name, waiters)
            yield
        finally:
            with self._nested_wait_lock:
                self._nested_waiters -= 1

    def _report_nested_wait(self, task_name: str, waiters: int) -> None:
        capacity = self._max_threads or 1
        queued = self.queued_tasks

        # Every worker is parked waiting on this pool and there is queued work none of
        # them can reach. This is not a risk any more, it has happened.
        if waiters >= capacity and queued > 0:
            self.logger.error(
                'Pool %s is deadlocked: all %d worker thread(s) are blocked waiting on '
                'tasks from this same pool, and %d task(s) are queued with no worker left '
                'to run them. Waiting on: %s. Submit the nested work to a different pool, '
                'or wait for it from a thread that is not one of this pool\'s workers.',
                self.name, capacity, queued, task_name)
            return

        now = time.monotonic()
        if now - self._last_nested_wait_warning < _NESTED_WAIT_WARN_INTERVAL_SECONDS:
            return
        self._last_nested_wait_warning = now

        self.logger.warning(
            'Pool %s has a worker blocked waiting on a task from its own pool (%s). '
            '%d of %d worker(s) are waiting this way, %d task(s) queued. This wastes a '
            'worker and deadlocks the pool if every worker does it.',
            self.name, task_name, waiters, capacity, queued)

    def enqueue_task(self, entry) -> None:
        """Queue a task, applying capacity only to producers outside the pool.

        A worker submitting onto its own pool is never made to wait. Throttling it would
        block the thread that has to drain the queue for the wait to end, which is a
        deadlock no timeout can recover from. Outside producers still get backpressure,
        which is what the bound is for.

        The room check and the put are deliberately not atomic, so concurrent producers
        can overshoot the capacity slightly. It is a backpressure threshold, not an
        invariant, and making it exact would reintroduce a lock held across a put.
        """
        # Fast path. Below capacity there is nothing to decide, so neither the worker
        # check nor the condition lock is touched; submitting is hot enough that paying
        # for them on every call cost 13.0 -> 14.7 us per add_task.
        if self.tasks.qsize() < self._queue_capacity:
            self.tasks.put(entry)
            return

        if not self.called_from_pool_worker():
            self._wait_for_queue_room()

        self.tasks.put(entry)

    def _wait_for_queue_room(self) -> None:
        """Block an outside producer until the queue is below capacity."""
        with self._queue_room:
            self._producers_waiting += 1
            try:
                waited = 0.0
                warned = False

                while self.tasks.qsize() >= self._queue_capacity:
                    if self.shutdown_event.is_set():
                        return

                    self._queue_room.wait(_QUEUE_ROOM_POLL_SECONDS)
                    waited += _QUEUE_ROOM_POLL_SECONDS

                    if not warned and waited >= _QUEUE_FULL_WARN_SECONDS:
                        warned = True
                        self.logger.warning(
                            'Pool %s has been full for %.0f seconds: %d queued against '
                            'a capacity of %d, %d running, %d worker threads. The '
                            'caller is blocked waiting for room.',
                            self.name, waited, self.tasks.qsize(),
                            self._queue_capacity, self.active_tasks,
                            len(self._threads))
            finally:
                self._producers_waiting -= 1

    def shutdown(self):
        if self.shutdown_event.is_set():
            return

        self.wait_completion()
        self.shutdown_event.set()

        # Release any producer still waiting for room so shutdown cannot strand it.
        with self._queue_room:
            self._queue_room.notify_all()

        nornir_pools._remove_pool(self)

        # The static atexit method gives threads time to die gracefully
        self._threads.clear()

    @abstractmethod
    def add_worker_thread(self):
        raise NotImplementedError("add_worker_thread must be implemented by derived class and return a thread object")

    def add_threads_if_needed(self):

        assert (self.shutdown_event.is_set() is False)

        self.remove_finished_threads()
        num_active_threads = len(self._threads)

        max_t = self._max_threads or 1
        if num_active_threads == max_t:
            return

        num_threads_needed = min(max_t, self.tasks.qsize() + 1) - num_active_threads

        num_threads_created = 0
        # while num_active_threads < min((self._max_threads, self.tasks.qsize()+1)):
        while num_threads_created < num_threads_needed:
            if not self.tasks.empty():
                t = self.add_worker_thread()
                assert (isinstance(t, threading.Thread))
                self._threads.append(t)
                num_active_threads += 1
                num_threads_created += 1
                time.sleep(0)

            else:
                break

    def remove_finished_threads(self):
        try:
            while True:
                t = self.deadthreadqueue.get_nowait()
                if t is None:
                    break
                else:
                    for i in range(len(self._threads) - 1, -1, -1):
                        if t == self._threads[i]:
                            del self._threads[i]
                            break
        except queue.Empty:
            pass

        return

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue.
           Note that wait or wait_return must be called on 
           each task to detect exceptions if there were any
           """

        self.tasks.join()
        self.remove_finished_threads()
