"""Pytest hooks for nornir-pools tests.

``MultiprocessThreadPool`` unpickles callables defined in ``tests.*`` inside
``ForkServerPoolWorker`` processes. Those workers start a fresh interpreter that
only sees ``os.environ["PYTHONPATH"]`` (and defaults), not necessarily the
``sys.path`` tweaks from the pytest parent process. Ensure the ``nornir-pools``
repo root (parent of the ``tests`` package) is on ``PYTHONPATH`` so
``import tests.test_nornir_pools`` succeeds in workers — including IDE
"Debug Test" runs where ``.vscode/settings.json`` only sets ``PYTHONPATH`` on
Windows terminals.
"""
from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import pytest

_POOL_ROOT = Path(__file__).resolve().parents[1]
_POOL_ROOT_S = str(_POOL_ROOT)
_PER_TEST_TIMEOUT_SECONDS = 180


def pytest_configure(config) -> None:
    if _POOL_ROOT_S not in sys.path:
        sys.path.insert(0, _POOL_ROOT_S)
    sep = os.pathsep
    cur = os.environ.get("PYTHONPATH", "")
    parts = [x for x in cur.split(sep) if x]
    if _POOL_ROOT_S not in parts:
        os.environ["PYTHONPATH"] = _POOL_ROOT_S + (sep + cur if cur else "")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    """Apply a default 3-minute timeout to each nornir-pools test."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def _timeout_handler(signum, frame):
        raise TimeoutError(
            f"Test exceeded {_PER_TEST_TIMEOUT_SECONDS}s timeout: {item.nodeid}"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, _PER_TEST_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
