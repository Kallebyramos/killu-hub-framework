# Medallion Architecture & Batch Modes — Study Guide

This document covers the `core/batch.py` primitives and the `layers/` package that implements the **Bronze → Silver** medallion architecture with full and incremental batch modes.

---

## Table of contents

1. [What is the medallion architecture?](#1-what-is-the-medallion-architecture)
2. [Batch modes — core concept](#2-batch-modes--core-concept)
3. [core/batch.py — BatchMode, BatchConfig, IncrementalState](#3-corebatchpy--batchmode-batchconfig-incrementalstate)
4. [Bronze layer — raw ingestion](#4-bronze-layer--raw-ingestion)
5. [Bronze metadata columns](#5-bronze-metadata-columns)
6. [Silver layer — transformation](#6-silver-layer--transformation)
7. [Silver standard transforms](#7-silver-standard-transforms)
8. [Incremental state management](#8-incremental-state-management)
9. [JsonFileStateStore vs IcebergStateStore](#9-jsonfilestatestore-vs-icebergstatestore)
10. [Full vs Incremental — decision guide](#10-full-vs-incremental--decision-guide)
11. [End-to-end flow](#11-end-to-end-flow)

---

## 1. What is the medallion architecture?

The medallion architecture (also called multi-hop or lakehouse architecture) organises data into quality tiers. Each tier is a set of Iceberg tables:

```
Source systems
    ↓
Bronze  (raw, immutable, append-only)
    ↓
Silver  (cleaned, typed, deduplicated, enriched)
    ↓
Gold    (aggregated, business-ready — future)
```

### Why three layers?

**Without layers**, data teams face a common problem: the pipeline that reads from Postgres and writes to Iceberg does everything at once — it filters, casts, deduplicates, and aggregates. When something breaks, it's impossible to tell whether the problem was in ingestion, transformation, or aggregation. Reprocessing requires re-reading from the source.

**With Bronze**, you separate concerns:
- Bronze always stores the raw record exactly as it came from the source. It is the immutable source of truth for your data lake.
- If a Silver transformation has a bug, you fix it and reprocess Silver from Bronze — without touching the source database again.
- Bronze tables are the audit trail: you can always prove what data you received and when.

**Silver** is where data becomes usable:
- Strings become proper timestamps
- Columns get the right types (`amount` → `DOUBLE`, not `STRING`)
- Duplicates are removed
- Date partition columns are derived for efficient querying

---

## 2. Batch modes — core concept

A **batch mode** controls which records a pipeline processes in a given run.

### FULL

Reads all available records from the source and (over)writes the target. Use when:
- Running for the first time (initial load)
- Fixing a bug that corrupted Silver — reprocess from Bronze
- Source doesn't have a reliable change timestamp
- The dataset is small enough to reprocess every time

```
Bronze table (10M rows)
    ↓ READ ALL
Silver table ← OVERWRITE
```

### INCREMENTAL

Reads only records newer than the last successful run's watermark. Use when:
- Bronze is large and grows continuously (Kafka events, CDC)
- You want low latency (process new data every 15 minutes)
- Source has a reliable timestamp column (`updated_at`, `event_time`, `_ingested_at`)

```
Bronze table (10M rows + 50K new since last run)
    ↓ WHERE _ingested_at > '2024-01-15 09:00:00'
    50K rows
Silver table ← APPEND
```

The watermark (`2024-01-15 09:00:00` in the example) is stored by the `IncrementalStateStore` and updated after every successful run.

---

## 3. core/batch.py — BatchMode, BatchConfig, IncrementalState

**File:** [killuhub/core/batch.py](../../killuhub/core/batch.py)

### BatchMode

```python
class BatchMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
```

`str, Enum` as the base means:
- `BatchMode.FULL == "full"` → `True`
- `json.dumps({"mode": BatchMode.FULL})` works without a custom encoder
- State files store `"full"` or `"incremental"` as plain strings

### BatchConfig

```python
@dataclass
class BatchConfig:
    mode: BatchMode
    watermark_column: str = "updated_at"
    initial_watermark: str = "1970-01-01T00:00:00"
    batch_id: str = ""
```

`initial_watermark` = epoch means "process all records on first INCREMENTAL run" — the expected behaviour for a fresh pipeline.

`batch_id` is the idempotency key. If you re-run the same Bronze batch (same `batch_id`), Silver can detect it via the `_batch_id` metadata column and skip or deduplicate — without any special framework support.

### IncrementalState

```python
@dataclass
class IncrementalState:
    source_table: str
    target_table: str
    last_watermark: str   # ISO-8601 string
    updated_at: str = ""  # when this state was written
```

Watermarks are stored as ISO-8601 strings (not Python `datetime` objects) because:
- They round-trip cleanly through JSON files and Iceberg STRING columns
- No timezone conversion issues — everything stays UTC
- Spark's `TIMESTAMP 'ISO-string'` SQL literal is parsed correctly across all catalog implementations

---

## 4. Bronze layer — raw ingestion

**File:** [killuhub/layers/bronze/pipeline.py](../../killuhub/layers/bronze/pipeline.py)

```
BronzeConfig
    ├── pipeline_config   (connector + engine — reuses existing PipelineConfig)
    ├── batch_config      (mode + watermark)
    ├── bronze_table      ("local.bronze.orders")
    └── source_name       ("postgres.shop.orders")
```

`BronzePipeline.run()` does three things:
1. Resolves a `batch_id` (generates UUID if not provided)
2. Builds a `_transform` closure that stamps metadata columns
3. Delegates everything else to the existing `Pipeline`

```python
# What BronzePipeline.run() does internally:
modified_config = PipelineConfig(
    ...original fields...,
    target_table=cfg.bronze_table,
    write_mode="append",               # always append
    transform_fn=bronze_transform,     # injects metadata columns
    storage_kwargs={"partition_by": ["_ingestion_date"]},
)
Pipeline(modified_config).run()
```

**Why always append?**

Bronze is the raw historical record. Modifying or deleting Bronze data means losing the audit trail. If the source sends a corrected record, Bronze gets both the original and the correction — Silver's deduplication (keeping the latest by `_ingested_at`) resolves the conflict.

**Why composition over inheritance?**

`BronzePipeline` composes `Pipeline` instead of extending it. If it inherited `Pipeline`, it would carry `PipelineConfig` as its interface — but `BronzePipeline` has fundamentally different concerns (no `target_table` in its config, always append mode). Composition lets `BronzeConfig` be a clean, purpose-built dataclass.

---

## 5. Bronze metadata columns

Every row written to a Bronze table has these five extra columns:

| Column            | Type      | Value                                   |
|-------------------|-----------|-----------------------------------------|
| `_ingested_at`    | TIMESTAMP | When Spark's current batch ran          |
| `_source_name`    | STRING    | e.g. `"postgres.shop.orders"`           |
| `_batch_id`       | STRING    | UUID — unique per pipeline invocation   |
| `_batch_mode`     | STRING    | `"full"` or `"incremental"`             |
| `_ingestion_date` | DATE      | Date-only truncation of `_ingested_at`  |

### How Silver uses them

- `_ingested_at` is the default `watermark_column` for Silver INCREMENTAL reads — it's always present regardless of source schema
- `_batch_id` enables re-run detection: if the same batch_id appears twice in Bronze, Silver can detect duplicates at the batch level
- `_ingestion_date` is the Bronze partition column — Spark can read a specific date partition without scanning all Bronze data

```sql
-- Silver INCREMENTAL reads only new Bronze rows
SELECT * FROM local.bronze.orders
WHERE _ingested_at > TIMESTAMP '2024-01-15 09:00:00'
ORDER BY _ingested_at ASC
```

---

## 6. Silver layer — transformation

**Files:** [killuhub/layers/silver/pipeline.py](../../killuhub/layers/silver/pipeline.py)

```
SilverConfig
    ├── bronze_table          (source Iceberg table)
    ├── silver_table          (target Iceberg table)
    ├── batch_config          (FULL or INCREMENTAL)
    ├── watermark_column      (default: "_ingested_at")
    ├── key_columns           (for deduplication)
    ├── date_columns          (strings to parse as timestamps)
    ├── date_dimension_column (add year/month/day/hour/date columns)
    ├── type_map              (column → Spark SQL type)
    ├── rename_map            (rename columns before writing)
    ├── drop_columns          (remove columns before writing)
    ├── null_check_columns    (drop rows with nulls here)
    ├── user_transform_fn     (custom Spark transform)
    ├── partition_by          (Silver partition columns)
    └── state_store           (JsonFileStateStore or IcebergStateStore)
```

`SilverPipeline` does not use a connector — it reads from Iceberg directly:

```python
# FULL: read everything
df = spark.table("local.bronze.orders")

# INCREMENTAL: read only new records
df = spark.sql("""
    SELECT * FROM local.bronze.orders
    WHERE _ingested_at > TIMESTAMP '2024-01-15 09:00:00'
    ORDER BY _ingested_at ASC
""")
```

The `ORDER BY _ingested_at ASC` is deliberate: if a run is interrupted and retried, the records are processed in chronological order, making the watermark advance predictably.

### The transform pipeline

Silver applies transforms in this fixed order:

```
1. add_metadata_columns    → _processed_at, _layer, _run_id
2. parse_date_columns      → string → TimestampType
3. add_date_dimensions     → year, month, day, hour, date
4. cast_types              → amount → double, etc.
5. rename_columns          → old_name → new_name
6. drop_columns            → remove unwanted fields
7. filter_nulls            → drop rows with nulls in key columns
8. deduplicate             → keep latest by key_columns + _ingested_at
9. user_transform_fn       → custom business logic (applied last)
```

Standard transforms (1-8) are optional — if the config list/map is empty, the step is skipped. The user transform is always last so it operates on already-cleaned data.

### Write mode per batch mode

| BatchMode     | Silver write mode | Why                                          |
|---------------|-------------------|----------------------------------------------|
| `FULL`        | `overwrite`       | Reprocessing all Bronze → replace all Silver |
| `INCREMENTAL` | `append`          | Adding new records → don't touch existing Silver |

---

## 7. Silver standard transforms

**File:** [killuhub/layers/silver/transformations.py](../../killuhub/layers/silver/transformations.py)

### `parse_date_columns`

```python
df = parse_date_columns(df, columns=["created_at", "updated_at"],
                            input_format="yyyy-MM-dd HH:mm:ss")
```

Converts strings like `"2024-01-15 10:30:00"` to Spark `TimestampType`. Uses `F.to_timestamp(col, format)`. Columns not present in the schema are silently skipped — safe after schema evolution.

Common formats:
- `"yyyy-MM-dd HH:mm:ss"` → `"2024-01-15 10:30:00"` (default)
- `"yyyy-MM-dd'T'HH:mm:ss"` → ISO 8601 `"2024-01-15T10:30:00"`
- `"dd/MM/yyyy"` → `"15/01/2024"`

### `add_date_dimensions`

```python
df = add_date_dimensions(df, source_column="created_at", prefix="order_")
# Adds: order_year, order_month, order_day, order_hour, order_date
```

These columns are used for:
- **Partitioning**: `partition_by=["order_date"]` lets Trino/Spark skip irrelevant partitions
- **BI tooling**: Tableau, Power BI, Metabase all work better with explicit year/month/day columns
- **Aggregations**: `GROUP BY order_year, order_month` without date parsing at query time

### `cast_types`

```python
df = cast_types(df, {
    "amount": "double",
    "quantity": "int",
    "price": "decimal(18,4)",
    "active": "boolean",
    "event_time": "timestamp",
})
```

Bronze stores everything as strings (because raw data may have format inconsistencies). Silver applies the correct types. `decimal(18,4)` is the correct type for financial amounts — `double` loses precision.

### `deduplicate`

```python
df = deduplicate(df, key_columns=["order_id"], order_column="_ingested_at", keep="latest")
```

Uses a window function:
```python
Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
# → row_number() == 1 is the most recently ingested version
```

Why not `df.dropDuplicates(["order_id"])`?

`dropDuplicates` keeps an arbitrary duplicate — whichever partition happens to process it first. `deduplicate()` with `keep="latest"` guarantees you keep the most recently ingested version, which is the correct behaviour for CDC (Change Data Capture) scenarios where the source sends updates.

### `filter_nulls`

```python
df = filter_nulls(df, columns=["order_id", "customer_id"])
```

Drops rows where any of the specified columns is null. Use this on your primary key columns to avoid Iceberg MERGE failures and ensure referential integrity.

### `apply_transforms` composer

```python
df = apply_transforms(df, [
    lambda d: parse_date_columns(d, ["created_at"]),
    lambda d: cast_types(d, {"amount": "double"}),
    my_custom_transform,
    None,   # ← None entries are skipped
])
```

Pipes the DataFrame through steps in order. None entries are no-ops. This is how `SilverPipeline` chains all the optional standard transforms without a jungle of if/else statements.

---

## 8. Incremental state management

**File:** [killuhub/layers/silver/state.py](../../killuhub/layers/silver/state.py)

The `IncrementalStateStore` is the mechanism that makes INCREMENTAL mode work across runs. After each successful Silver run, the store saves:

```json
{
    "local.bronze.orders::local.silver.orders": {
        "source_table": "local.bronze.orders",
        "target_table": "local.silver.orders",
        "last_watermark": "2024-01-15T10:30:00+00:00",
        "updated_at": "2024-01-15T10:31:05+00:00"
    }
}
```

On the next run, Silver reads `last_watermark` and filters:
```sql
WHERE _ingested_at > TIMESTAMP '2024-01-15T10:30:00+00:00'
```

### State update after FULL run

Even after a FULL Silver run, the watermark is updated to `max(_ingested_at)` of the processed Bronze data. This means the next INCREMENTAL run (if you switch modes) correctly picks up only Bronze records added after the FULL reprocess — it doesn't re-process the entire Bronze table again.

---

## 9. JsonFileStateStore vs IcebergStateStore

### JsonFileStateStore (development)

```python
store = JsonFileStateStore(path=".killuhub_state.json")
```

- Stored as a local JSON file
- Atomic writes using `tempfile` + `os.replace()` — crash-safe
- Zero dependencies — works everywhere
- **Not safe across multiple processes or machines**
- Best for: local development, single-machine pipelines, testing

### IcebergStateStore (production)

```python
store = IcebergStateStore(
    spark=spark_session,
    state_table="local.meta.killuhub_state",
    catalog_name="local",
)
```

- State stored in a dedicated Iceberg table (one row per table pair)
- Uses `MERGE INTO` — ACID, safe across concurrent processes
- State visible to any engine that can query Iceberg (Spark, Trino, etc.)
- Requires an active SparkSession
- Best for: production, Databricks, multi-engine environments

### Dependency injection

`SilverConfig.state_store` accepts any `IncrementalStateStore`:

```python
# Dev: auto-created if not specified
SilverConfig(state_store=None, ...)            # → JsonFileStateStore(".killuhub_state.json")

# Custom path
SilverConfig(state_store=JsonFileStateStore("/opt/killuhub/state.json"), ...)

# Production
SilverConfig(state_store=IcebergStateStore(spark, "prod.meta.state"), ...)

# Testing
class MockStateStore(IncrementalStateStore):
    def load(self, *args): return None
    def save(self, state): ...

SilverConfig(state_store=MockStateStore(), ...)
```

---

## 10. Full vs Incremental — decision guide

```
Is this the first run?
    YES → FULL (initial load)
    NO  ↓

Does the Bronze table fit in one Spark job comfortably?
    YES → FULL is fine
    NO  ↓

Does Bronze have a reliable monotonic column (_ingested_at always works)?
    YES → INCREMENTAL
    NO  → FULL

Do you need low latency (new data processed every few minutes)?
    YES → INCREMENTAL
    NO  → FULL is simpler

Did you fix a Silver bug and need to reprocess?
    YES → FULL (then switch back to INCREMENTAL)
```

### Incremental is idempotent

If an INCREMENTAL run partially succeeds (processes 30K rows, writes them, then crashes before updating the watermark), the next run will reprocess those same 30K rows. Because Silver uses `deduplicate(key_columns=["order_id"])`, the re-processed rows will overwrite the existing ones with identical data — no duplicates in the output.

The state update happens **after** a successful write for exactly this reason. The write must be committed before the watermark advances.

---

## 11. End-to-end flow

```
Postgres (orders table)
    │
    ▼  BronzePipeline.run()
    ├── Pipeline(connector=postgres, engine=spark, writer=iceberg)
    │       ├── PostgresConnector.extract() → yields raw dicts
    │       ├── SparkEngine.create_dataframe(batch)
    │       ├── bronze_transform(df):
    │       │       df.withColumn("_ingested_at",   current_timestamp())
    │       │         .withColumn("_source_name",   lit("postgres.shop.orders"))
    │       │         .withColumn("_batch_id",      lit("uuid-abc-123"))
    │       │         .withColumn("_batch_mode",    lit("full"))
    │       │         .withColumn("_ingestion_date", to_date(current_timestamp()))
    │       └── IcebergWriter.write(df, "local.bronze.orders", "append")
    │
    ▼  local.bronze.orders (raw + metadata)
    │   order_id | amount | created_at | _ingested_at          | _batch_id    | ...
    │   1        | 99.0   | "2024-01-15"| 2024-01-15 10:00:00  | uuid-abc-123 |
    │   2        | 149.5  | "2024-01-15"| 2024-01-15 10:00:00  | uuid-abc-123 |
    │
    ▼  SilverPipeline.run() (INCREMENTAL)
    ├── spark.sql("SELECT * FROM local.bronze.orders WHERE _ingested_at > '1970...'")
    ├── apply_transforms:
    │       add_metadata_columns  → _processed_at, _layer, _run_id
    │       parse_date_columns    → created_at: "2024-01-15" → timestamp
    │       add_date_dimensions   → order_year=2024, order_month=1, order_day=15, ...
    │       cast_types            → amount: "99.0" → 99.0 (DOUBLE)
    │       deduplicate           → keep latest by order_id
    │       user_transform_fn     → filter("amount > 0")
    ├── IcebergWriter.write(df, "local.silver.orders", "append")
    └── state_store.save(watermark="2024-01-15T10:00:00")
    │
    ▼  local.silver.orders (clean + enriched)
        order_id | amount | created_at          | order_year | order_month | _processed_at | ...
        1        | 99.0   | 2024-01-15 00:00:00 | 2024       | 1           | 2024-01-15 10:01:00 |
        2        | 149.5  | 2024-01-15 00:00:00 | 2024       | 1           | 2024-01-15 10:01:00 |
```

Next Silver run (INCREMENTAL): reads only rows with `_ingested_at > '2024-01-15T10:00:00'` — the rows from the first batch are untouched.
