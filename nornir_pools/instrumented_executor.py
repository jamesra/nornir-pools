"""ThreadPoolExecutor that publishes dashboard pool_load telemetry."""
from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from nornir_shared.pool_load import report_pool_load


class InstrumentedThreadPoolExecutor(ThreadPoolExecutor):
    """``ThreadPoolExecutor`` that reports load under a stable pool *name*.

    Use this (or call :func:`nornir_shared.pool_load.report_pool_load` manually)
    so ad hoc executors appear in the dashboard Pools section alongside
    ``nornir_pools`` pools.
    """

    def __init__(
        self,
        name: str,
        max_workers: int | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(max_workers=max_workers, *args, **kwargs)
        self.pool_name = str(name or "").strip() or "ThreadPoolExecutor"
        self._queued = 0
        self._active = 0
        self._lock = threading.Lock()
        self._max_workers = max_workers

    def _publish(self) -> None:
        with self._lock:
            queued = self._queued
            active = self._active
        report_pool_load(
            self.pool_name,
            queued=queued,
            active=active,
            max_workers=self._max_workers,
        )

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        with self._lock:
            self._queued += 1
        self._publish()

        def _wrapped(*a: Any, **kw: Any) -> Any:
            with self._lock:
                if self._queued > 0:
                    self._queued -= 1
                self._active += 1
            self._publish()
            try:
                return fn(*a, **kw)
            finally:
                with self._lock:
                    if self._active > 0:
                        self._active -= 1
                self._publish()

        return super().submit(_wrapped, *args, **kwargs)
