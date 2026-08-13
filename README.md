# nornir-pools

Thread, process, and clustering pools with a shared interface for local or distributed execution.

## Mental model

- Every `add_task` / `add_process` returns a **Task**. Callers always `wait` /
  `wait_return` (or shut down the pool) so work drains.
- **Thread pools** — Python callables and I/O-bound work (`GetGlobalThreadPool`).
- **Process / multiprocess pools** — CPU-bound Python callables
  (`GetGlobalMultiprocessPool` / local machine pool).
- **Subprocess / ProcessPool** — **shell commands and external binaries** via
  string/`Popen` args (`GetGlobalProcessPool`). Do **not** pass Python callables
  to `ProcessPool.add_task` (raises `NotImplementedError`); use a multiprocess
  or thread pool instead.
- **Parallel Python (cluster)** — multi-machine Python callables
  (`GetGlobalClusterPool`). Prefer only when a live `pp` cluster is configured.

At pipeline stage boundaries call `nornir_pools.ReleaseStagePools()` so thread
pools recycle while process pools stay warm.

## Parallel Python callback timeout

`CTask.wait` no longer hangs forever when the remote callback never fires
(missing package / hung worker). After `server.wait` returns it waits a primary
timeout (default 300s), then a secondary bound (default 60s). If the callback
still has not arrived, `ActiveJobCount` is unwound once and `wait` raises
`RuntimeError`. Tests: `tests/test_parallelpython_callback_timeout.py`.

## Documentation

- **Full manual and API (umbrella):** [https://nornir.github.io/](https://nornir.github.io/)
- **This package:** [Packages — nornir-pools](https://nornir.github.io/packages/nornir_pools.html)
- **API reference:** [`nornir_pools` module](https://nornir.github.io/api/nornir_pools.html)
