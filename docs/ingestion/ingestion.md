# Ingestion Layer — Study Guide

The ingestion layer is KilluHub's orchestration brain. It ties all other layers together: it takes a `PipelineConfig`, resolves the right connector/engine/writer from the Registry, and drives the full extract-transform-load cycle.

Two components live here: **Pipeline** (runs one ETL cycle) and **PipelineScheduler** (runs pipelines on a schedule).

---

## Table of contents

1. [Pipeline architecture](#1-pipeline-architecture)
2. [Pipeline.run() — step by step](#2-pipelinerun--step-by-step)
3. [Batching with islice](#3-batching-with-islice)
4. [Error handling and the PipelineError wrapper](#4-error-handling-and-the-pipelineerror-wrapper)
5. [How the writer is resolved](#5-how-the-writer-is-resolved)
6. [PipelineScheduler — how it works](#6-pipelinescheduler--how-it-works)
7. [APScheduler internals](#7-apscheduler-internals)
8. [Cron expressions](#8-cron-expressions)
9. [Production patterns](#9-production-patterns)
10. [End-to-end flow diagram](#10-end-to-end-flow-diagram)

---

## 1. Pipeline architecture

```
Pipeline
    │
    ├── PipelineConfig          (what to run)
    ├── BaseConnector instance  (where data comes from)
    ├── BaseEngine instance     (how to process it)
    └── BaseStorageWriter instance (where data goes)
```

Pipeline is stateful during a single run: it holds references to the connector, engine, and writer as instance variables. After `run()` completes (or fails), `engine.stop()` is always called in a `finally` block.

The Pipeline itself has no knowledge of Postgres, Spark, or Iceberg. It only talks to the abstract interfaces — all concreteness is resolved via the Registry at runtime.

---

## 2. Pipeline.run() — step by step

**File:** [killuhub/ingestion/pipeline.py](../../killuhub/ingestion/pipeline.py)

```python
def run(self) -> dict[str, Any]:
    # 1. Resolve classes from registry
    connector_cls = default_registry.get_connector(self.config.connector_name)
    engine_cls    = default_registry.get_engine(self.config.engine_name)

    # 2. Instantiate components
    self._connector = connector_cls(self.config.connector_config)
    self._engine    = engine_cls()
    self._engine.init_session(
        app_name=f"killuhub-{self.config.connector_name}",
        **self.config.engine_kwargs,
    )
    self._writer = self._build_writer()

    # 3. Stream records in batches
    with self._connector:                     # calls connect() / close()
        records_iter = self._connector.extract()
        while True:
            batch = list(islice(records_iter, self.config.batch_size))
            if not batch:
                break

            df = self._engine.create_dataframe(batch)
            df = self._engine.apply_transform(df, self.config.transform_fn)
            self._writer.write(df, self.config.target_table, self.config.write_mode)

    # 4. Return summary
    return {"total_records": total_records, "total_batches": total_batches, ...}
```

The loop runs until `islice` returns an empty list, which happens when the connector's iterator is exhausted.

### Why `with self._connector`?

The `with` statement calls `connector.connect()` on entry and `connector.close()` on exit — even if the loop crashes. This guarantees resource cleanup (database connections, Kafka consumers, HTTP sessions).

---

## 3. Batching with islice

`islice` is from Python's `itertools` module. It lazily takes the first N items from any iterator:

```python
from itertools import islice

# Imagine extract() yields 100,000 records
records_iter = connector.extract()

# First batch: items 0..4999
batch1 = list(islice(records_iter, 5000))  # [record0, record1, ..., record4999]

# Second batch: items 5000..9999 (iterator state is preserved)
batch2 = list(islice(records_iter, 5000))  # [record5000, ..., record9999]

# When exhausted: empty list
batch_n = list(islice(records_iter, 5000))  # []  ← loop exits
```

The key insight: `islice` doesn't restart the iterator. The iterator state (which database cursor position, which Kafka offset, which file position) is maintained between calls. Each `islice` call picks up exactly where the previous one left off.

### Why batching matters

Without batching, you'd have two bad options:

**Option A: one record at a time**
```
for record in connector.extract():
    df = spark.createDataFrame([record])
    writer.write(df, ...)
```
Creating a Spark DataFrame for a single row is extremely expensive — it starts a JVM task for each record. 1M records = 1M Spark jobs.

**Option B: all records at once**
```
records = list(connector.extract())  # loads 10M rows into RAM
df = spark.createDataFrame(records)
```
Crashes on large tables (OOM).

**Batching is the balance**: create a reasonably-sized DataFrame (10K rows) per Spark job, giving the engine meaningful work without exhausting memory.

### Choosing batch_size

| Source          | Recommended batch_size | Reasoning                           |
|-----------------|------------------------|-------------------------------------|
| Postgres        | 5,000–50,000           | Large enough to amortize Spark startup |
| MySQL           | 1,000–10,000           | `fetchmany` size, tune per table    |
| Kafka           | 10,000–100,000         | Match your topic throughput         |
| REST API        | Equals page_size       | One batch = one API page            |

---

## 4. Error handling and the PipelineError wrapper

```python
try:
    # ... full pipeline logic
except Exception as exc:
    raise PipelineError(f"Pipeline failed: {exc}") from exc
finally:
    if self._engine:
        self._engine.stop()
```

### `raise X from Y` — exception chaining

`from exc` preserves the original exception as the `__cause__` of `PipelineError`. When you see the traceback, you get both:

```
killuhub.core.exceptions.PipelineError: Pipeline failed: connection refused
  
The above exception was the direct cause of the following exception:

psycopg2.OperationalError: connection refused to host "localhost"
```

This is far more useful for debugging than swallowing the original error.

### `finally` guarantees engine cleanup

The `finally` block runs regardless of whether the pipeline succeeded or failed. Without it, a failed pipeline would leave a Spark JVM running in the background, consuming memory and ports.

---

## 5. How the writer is resolved

Writers are different from connectors and engines — they require the engine's session object (a `SparkSession` or Flink `TableEnvironment`) to operate. They can't be registered in the same registry because they're not independent classes.

KilluHub uses a private dict instead:

```python
_WRITER_CLASSES = {
    "iceberg": IcebergWriter,
    "delta":   DeltaWriter,
    "hudi":    HudiWriter,
}

def _build_writer(self):
    writer_name = self.config.storage_writer_name
    writer_cls  = _WRITER_CLASSES[writer_name]
    
    # Get the engine's native session
    session = getattr(self._engine, "spark", None) \
           or getattr(self._engine, "table_env", None)
    
    return writer_cls(session, **self.config.storage_kwargs)
```

The `getattr` calls probe the engine for known session attributes (`spark` on SparkEngine, `table_env` on FlinkEngine). New engine types just need to expose their session via a known attribute name.

---

## 6. PipelineScheduler — how it works

**File:** [killuhub/ingestion/scheduler.py](../../killuhub/ingestion/scheduler.py)

The `PipelineScheduler` wraps **APScheduler** and provides a KilluHub-native API for scheduling pipeline runs.

```python
scheduler = PipelineScheduler()

# Cron: every hour at minute 0
scheduler.add_cron_job("hourly_sync", config, "0 * * * *")

# Interval: every 15 minutes
scheduler.add_interval_job("fast_sync", config, minutes=15)

scheduler.start()  # blocks until Ctrl+C
```

### How the scheduler holds job config

```python
self._jobs: dict[str, PipelineConfig] = {}

def add_cron_job(self, job_id, config, cron_expression):
    self._jobs[job_id] = config        # store config by ID
    self._scheduler.add_job(
        func=self._run_job,            # APScheduler calls this
        kwargs={"job_id": job_id},     # passes the ID
        trigger="cron",
        ...
    )
```

When APScheduler fires a job, it calls `_run_job(job_id="hourly_sync")`. The method looks up the `PipelineConfig` by ID and creates a fresh `Pipeline` instance for each run:

```python
def _run_job(self, job_id: str) -> None:
    config = self._jobs[job_id]
    Pipeline(config).run()
```

A fresh `Pipeline` instance per run means no state leaks between executions. Each run gets a clean Spark session, a fresh connector, and its own error handling.

### Blocking vs non-blocking start

```python
scheduler.start(blocking=True)   # sleeps in main thread until Ctrl+C
scheduler.start(blocking=False)  # returns immediately, scheduler runs in background thread
```

`blocking=False` is useful when embedding the scheduler inside a FastAPI app or a larger service:

```python
app = FastAPI()

@app.on_event("startup")
def start_scheduler():
    scheduler.start(blocking=False)  # runs in background
```

---

## 7. APScheduler internals

APScheduler's `BackgroundScheduler` runs jobs in a thread pool (by default, a `ThreadPoolExecutor`). Each job runs in a separate thread, so multiple jobs can run in parallel.

```
Main thread           Background scheduler thread    Worker thread(s)
     │                          │                          │
scheduler.start() ──▶  starts event loop                  │
     │                          │                          │
     │               trigger fires "hourly_sync"           │
     │                          ├──────────────────▶ _run_job("hourly_sync")
     │                          │                    Pipeline(config).run()
     │               trigger fires "fast_sync"             │
     │                          ├──────────────────▶ _run_job("fast_sync")
     │                          │                    Pipeline(config).run()
```

### Important: Spark and threading

Spark is not fully thread-safe. If two jobs use the same `SparkSession` simultaneously, you may get conflicts. KilluHub's design avoids this because each `Pipeline.run()` creates its own `SparkSession` via `getOrCreate()`.

In practice, if you run multiple Spark pipelines simultaneously from the same process, Spark serializes them internally (one runs while the other waits for the `getOrCreate()` lock). For high-frequency parallel jobs, consider separate processes or a proper orchestrator (Airflow, Prefect).

---

## 8. Cron expressions

A cron expression has 5 space-separated fields:

```
┌───────── minute        (0 - 59)
│ ┌─────── hour          (0 - 23)
│ │ ┌───── day of month  (1 - 31)
│ │ │ ┌─── month         (1 - 12)
│ │ │ │ ┌─ day of week   (0 - 6, 0=Sunday)
│ │ │ │ │
* * * * *
```

| Expression    | Meaning                              |
|---------------|--------------------------------------|
| `* * * * *`   | Every minute                         |
| `0 * * * *`   | Every hour (at minute 0)             |
| `0 6 * * *`   | Every day at 6:00 AM                 |
| `0 6 * * 1`   | Every Monday at 6:00 AM              |
| `*/15 * * * *`| Every 15 minutes                     |
| `0 0 1 * *`   | First day of every month at midnight |
| `0 8-18 * * 1-5` | Every hour from 8-6 PM on weekdays |

For simple intervals (every N minutes/hours), prefer `add_interval_job` — it's easier to read:

```python
# These are equivalent:
scheduler.add_cron_job("job", config, "*/15 * * * *")
scheduler.add_interval_job("job", config, minutes=15)
```

---

## 9. Production patterns

### Idempotent pipelines

A pipeline should produce the same result whether it runs once or ten times. The key is using `write_mode="merge"` with unique keys:

```python
PipelineConfig(
    write_mode="merge",
    storage_kwargs={"merge_keys": ["order_id"]},
)
```

If a scheduled job fails and retries, the merge ensures no duplicate rows appear.

### Incremental extraction

Instead of `SELECT * FROM orders`, use a timestamp to fetch only new/changed records:

```python
# Airflow-style: pass the execution window as a query parameter
query = f"""
    SELECT * FROM orders
    WHERE updated_at >= '{last_run_timestamp}'
      AND updated_at <  '{current_timestamp}'
"""

ConnectorConfig.from_dict({"query": query, ...})
```

Combined with `write_mode="merge"`, this gives you efficient incremental loads.

### Logging and monitoring

The Pipeline logs every batch with structured fields:

```
INFO Starting pipeline | connector=postgres engine=spark writer=iceberg table=local.shop.orders
INFO Batch 1 written   | records_in_batch=5000  total=5000
INFO Batch 2 written   | records_in_batch=5000  total=10000
INFO Batch 3 written   | records_in_batch=2847  total=12847
INFO Pipeline complete | {'connector': 'postgres', 'total_records': 12847, 'total_batches': 3}
```

In production, ship these logs to your observability stack (Datadog, Grafana, CloudWatch). The summary dict returned by `run()` can be forwarded to a metrics endpoint or stored in a pipeline runs table for dashboarding.

### Handling transient failures in the scheduler

The scheduler catches all exceptions so one failed job doesn't crash the scheduler:

```python
def _run_job(self, job_id: str) -> None:
    try:
        Pipeline(config).run()
    except Exception:
        logger.exception("Job '%s' failed.", job_id)
        # ← scheduler continues running; next trigger will fire normally
```

For critical pipelines, extend this with alerting:

```python
class AlertingScheduler(PipelineScheduler):
    def _run_job(self, job_id: str) -> None:
        try:
            super()._run_job(job_id)
        except Exception as e:
            send_slack_alert(f"Pipeline {job_id} failed: {e}")
```

---

## 10. End-to-end flow diagram

```
User code
    │
    └── Pipeline(PipelineConfig).run()
            │
            ├── Registry.get_connector("postgres") ──▶ PostgresConnector
            ├── Registry.get_engine("spark")        ──▶ SparkEngine
            └── _build_writer("iceberg")            ──▶ IcebergWriter
                        │
                        ▼
            connector.connect()  [opens DB connection]
                        │
                        ▼
            ┌─────────────────────────────────────┐
            │          BATCH LOOP                  │
            │                                      │
            │  islice(connector.extract(), 10_000) │
            │         ▼                            │
            │  engine.create_dataframe(batch)      │ ◀── Spark creates DataFrame
            │         ▼                            │
            │  engine.apply_transform(df, fn)      │ ◀── user's Spark code runs
            │         ▼                            │
            │  writer.write(df, table, "append")   │ ◀── Iceberg writeTo().append()
            │                                      │
            │  repeat until extract() exhausted    │
            └─────────────────────────────────────┘
                        │
                        ▼
            connector.close()  [closes DB connection]
            engine.stop()      [shuts down SparkSession]
                        │
                        ▼
            return {"total_records": N, "total_batches": M, ...}
```

---

## Summary

| Component         | Responsibility                                                   |
|-------------------|------------------------------------------------------------------|
| `Pipeline`        | Orchestrates one full extract-transform-load cycle               |
| `PipelineConfig`  | Describes what to run — connector, engine, writer, table, mode  |
| `islice` batching | Streams records in memory-safe chunks; preserves iterator state  |
| `PipelineError`   | Wraps all runtime failures with chained tracebacks               |
| `PipelineScheduler` | Schedules pipeline runs on cron or interval via APScheduler    |
| `_run_job`        | Creates a fresh Pipeline per execution; isolates failures        |
