# Execution cancellation

SQL is planned lazily. Cancellation applies when a DataFrame action is running, such as
`collect()`, `count()`, `show()`, `toPandas()`, `toArrow()`, or `write.saveAsTable()`.

Plan the DataFrame on the SparkSession's owning thread, then pass a cancellation handle to a
control thread. `SparkSession` itself remains thread-bound:

```python
import sparkless
from concurrent.futures import ThreadPoolExecutor, TimeoutError

df = spark.sql("select ...")
cancellation = spark.cancellation_handle()

with ThreadPoolExecutor(max_workers=2) as pool:
    future = pool.submit(df.collect)
    try:
        rows = future.result(timeout=5)
    except TimeoutError:
        pool.submit(cancellation.cancel_all_active_actions).result()
        try:
            future.result()
        except sparkless.SparklessCancelledError:
            pass
```

`DataFrame.cancel()` requests cancellation for actions started through that DataFrame
object. `SparkSession.cancelAllActiveJobs()` requests cancellation for all active actions
started through the session and returns the number of active actions signaled. `stop()` also
signals the session's active actions before it clears session state.

Call `spark.cancellation_handle()` on the owning thread and pass the returned handle to a
control thread when cancellation must be requested from another thread. The handle can be
called as `cancel_all_active_actions()` or `cancelAllActiveJobs()`.

Cancellation is cooperative. The Polars backend checks before execution and after lazy
materialization. It cannot interrupt a `LazyFrame::collect()` call already in progress, so a
request made during that call becomes a `SparklessCancelledError` when the call returns and
the backend reaches its next safe point. Callers should wait for the action's terminal result
before releasing any admission lease or treating its work as finished.

For external catalog `saveAsTable()` writes, cancellation before the provider write begins
prevents the provider call. Once the provider write begins, Sparkless returns the provider's
receipt or error unchanged, even if cancellation arrives concurrently. Providers that can
check cancellation before their commit may override `write_table_with_cancellation`; they
must return a cancelled outcome only when the commit was not accepted.

Rust embedders can share `robin_sparkless_core::CancellationToken` with an action and call
`DataFrameBackend::collect_with_cancellation` or `count_with_cancellation`. Backends may add
safe points, while the default implementation checks before and after the action.
