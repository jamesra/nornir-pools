"""A ProcessPool command must launch exactly once, and the dead launch path is gone.

``ImmediateProcessTask`` carried two unreachable methods::

    def Run(self):
        self.proc = subprocess.Popen(self.cmd, stdout=..., stderr=..., *self.args, **self.kwargs)

    def _handle_proc_completion(self):
        self.stdoutdata = self.returned_value[0].decode('utf-8')
        self.stderrdata = self.returned_value[1].decode('utf-8')
        self.set_completion_time()
        self.completed.set()

``Worker.run`` already does all of this: it Popens ``entry.cmd``, decodes both streams,
sets ``returncode``, and marks completion. Neither method had a single caller anywhere in
the monorepo, and the constructor comment already warned "do not Popen here or the
command runs twice", so a double-spawn had been fixed once and its other half left in
place.

``Run`` was the trap: its name invites calling it, and doing so would run the command a
second time. ``_handle_proc_completion`` was worse than merely dead -- on a failed task
``Worker.run`` sets ``entry.returned_value = None``, so it would have raised
``TypeError`` on ``None[0]``. ``self.proc`` was assigned only by ``Run`` and read
nowhere, so it went too.

The tests below split into two groups. The removal group fails before the change and
guards against the names coming back. The behavioural group passes before and after by
design -- ``Run`` was never called, so nothing was observably broken -- and exists to
pin the exactly-once invariant that makes reintroducing the double-spawn a test failure
rather than a silent duplicate execution.
"""
from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import unittest

import nornir_pools as pools
import nornir_pools.processpool as processpool

POOL_NAME = 'Test single spawn pool'


def tearDownModule():
    pools.ClosePools()


class _AppenderFixture(unittest.TestCase):
    """A command that appends one line per execution, so spawns are countable."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

        self.target = os.path.join(self._tmp.name, 'spawns.txt')
        self.script = os.path.join(self._tmp.name, 'appender.py')
        with open(self.script, 'w', encoding='utf-8') as handle:
            handle.write(textwrap.dedent('''
                import sys
                with open(sys.argv[1], 'a', encoding='utf-8') as f:
                    f.write('spawned\\n')
                print('hello from the child')
                sys.exit(int(sys.argv[2]))
                '''))

    def _command(self, exit_code=0, target=None):
        target = self.target if target is None else target
        return f'"{sys.executable}" "{self.script}" "{target}" {exit_code}'

    def _distinct_target(self, index):
        """Each task appends to its own file.

        Pointing concurrent children at one shared file loses appends: three tasks
        against a single target counted 2 lines, because unsynchronised append-mode
        writes from separate processes are not reliably atomic on Windows. That is a
        property of the fixture, not of the pool, so give each task its own file and
        count files instead.
        """
        return os.path.join(self._tmp.name, f'spawns_{index}.txt')

    def _spawn_count(self, target=None):
        target = self.target if target is None else target
        if not os.path.exists(target):
            return 0
        with open(target, 'r', encoding='utf-8') as handle:
            return len([line for line in handle if line.strip()])


class TestCommandRunsExactlyOnce(_AppenderFixture):

    def setUp(self):
        super().setUp()
        self.pool = pools.GetProcessPool(POOL_NAME, 2)

    def test_a_single_add_process_spawns_once(self):
        task = self.pool.add_process('once', self._command())
        task.wait()

        self.assertEqual(self._spawn_count(), 1,
                         'the command must not be launched by both the task and the worker')

    def test_stdout_is_captured_once(self):
        task = self.pool.add_process('stdout', self._command())

        self.assertIn('hello from the child', task.wait_return())

    def test_three_commands_each_spawn_exactly_once(self):
        targets = [self._distinct_target(i) for i in range(3)]
        tasks = [self.pool.add_process(str(i), self._command(target=t))
                 for i, t in enumerate(targets)]
        for task in tasks:
            task.wait()

        counts = [self._spawn_count(t) for t in targets]

        self.assertEqual(counts, [1, 1, 1],
                         'every task must run once: a 0 means it never ran, a 2 means '
                         'it was launched twice')

    def test_a_failing_command_raises_and_still_spawned_once(self):
        task = self.pool.add_process('fails', self._command(exit_code=3))

        with self.assertRaises(Exception):
            task.wait()

        self.assertEqual(self._spawn_count(), 1)

    def test_a_failing_command_records_its_return_code(self):
        task = self.pool.add_process('fails-rc', self._command(exit_code=3))
        try:
            task.wait()
        except Exception:
            pass

        self.assertEqual(task.returncode, 3)


class TestConstructionDoesNotSpawn(_AppenderFixture):
    """Building the task object must not launch anything; the worker owns launch."""

    def test_constructing_the_task_spawns_nothing(self):
        processpool.ImmediateProcessTask('not-run', self._command(), shell=True)

        self.assertEqual(self._spawn_count(), 0)

    def test_the_command_is_only_stored(self):
        command = self._command()
        task = processpool.ImmediateProcessTask('stored', command, shell=True)

        self.assertEqual(task.cmd, command)


class TestDeadLaunchPathIsGone(unittest.TestCase):
    """These fail before the change."""

    def test_run_is_gone(self):
        self.assertFalse(hasattr(processpool.ImmediateProcessTask, 'Run'),
                         'Run() would Popen the command a second time')

    def test_handle_proc_completion_is_gone(self):
        self.assertFalse(
            hasattr(processpool.ImmediateProcessTask, '_handle_proc_completion'),
            'Worker.run already decodes output and marks completion')

    def test_the_vestigial_proc_attribute_is_gone(self):
        task = processpool.ImmediateProcessTask('attrs', 'echo hi', shell=True)

        self.assertFalse(hasattr(task, 'proc'),
                         'only Run() ever assigned proc, and nothing read it')

    def test_the_task_module_no_longer_popens(self):
        """Popen must appear only in Worker.run within this module."""
        import inspect

        source = inspect.getsource(processpool.ImmediateProcessTask)
        code = '\n'.join(line.split('#', 1)[0] for line in source.splitlines())

        self.assertNotIn('Popen', code)

    def test_the_sibling_task_class_also_does_not_popen(self):
        import inspect

        source = inspect.getsource(processpool.ProcessTask)
        code = '\n'.join(line.split('#', 1)[0] for line in source.splitlines())

        self.assertNotIn('Popen', code)


if __name__ == '__main__':
    unittest.main()
