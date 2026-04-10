# Storage Layer — Study Guide

The storage layer is where processed data lands permanently. KilluHub uses **Apache Iceberg** as its primary table format, with Apache Hudi and Delta Lake as pluggable alternatives. All three implement the same `BaseStorageWriter` interface.

This is one of the most important architectural decisions in any modern data platform — choosing the wrong table format can be costly to migrate from later.

---

## Table of contents

1. [The problem: why table formats exist](#1-the-problem-why-table-formats-exist)
2. [The three formats at a glance](#2-the-three-formats-at-a-glance)
3. [Apache Iceberg — deep dive](#3-apache-iceberg--deep-dive)
4. [IcebergWriter — implementation walkthrough](#4-icebergwriter--implementation-walkthrough)
5. [IcebergSchemaManager — schema evolution and maintenance](#5-icebergschemamanager--schema-evolution-and-maintenance)
6. [Delta Lake — overview and stub](#6-delta-lake--overview-and-stub)
7. [Apache Hudi — overview and stub](#7-apache-hudi--overview-and-stub)
8. [Format comparison deep-dive](#8-format-comparison-deep-dive)
9. [Why KilluHub chose Iceberg](#9-why-killuhub-chose-iceberg)
10. [Swapping storage writers](#10-swapping-storage-writers)

---

## 1. The problem: why table formats exist

Object storage (S3, GCS, ADLS) is cheap and scalable, but it's just a key-value store of files. Raw Parquet files on S3 have serious limitations:

- **No atomicity**: if you write 10 files and crash after 5, you have a corrupted table
- **No schema tracking**: you have to remember what columns each file has
- **No deletes or updates**: you'd have to rewrite the entire dataset
- **No concurrent writers**: two jobs writing simultaneously corrupt each other
- **No time travel**: can't query "what did this table look like last Tuesday?"
- **Slow metadata**: listing millions of files on S3 to find the right partition is slow

Table formats solve all of these problems by adding a **metadata layer** on top of regular Parquet/ORC/Avro files.

```
S3 bucket (raw files — unchanged)
├── data/orders/part-00001.parquet
├── data/orders/part-00002.parquet
└── metadata/ (the "table format" layer)
    ├── v1.metadata.json      ← snapshot history, schema, partition spec
    ├── snap-001.avro         ← manifest list for snapshot 1
    └── manifest-001.avro     ← which files belong to this snapshot
```

---

## 2. The three formats at a glance

```
Apache Iceberg   — designed by Netflix, open governance (Apache)
Delta Lake       — designed by Databricks, open source but Databricks-first
Apache Hudi      — designed by Uber, strong upsert support
```

All three give you ACID transactions on object storage. The differences are in the details.

---

## 3. Apache Iceberg — deep dive

**Files:** [killuhub/storage/iceberg/](../../killuhub/storage/iceberg/)

### Metadata architecture

Iceberg's metadata has three layers:

```
table metadata file (v3.metadata.json)
    └── manifest list (snap-3.avro)
            ├── manifest file A (manifest-a.avro)
            │       ├── data/part-001.parquet   (30,000 rows, id 1-30000)
            │       └── data/part-002.parquet   (25,000 rows, id 30001-55000)
            └── manifest file B (manifest-b.avro)
                    └── data/part-003.parquet   (15,000 rows, id 55001-70000)
```

- **Table metadata file**: the current state of the table — current schema, partition spec, list of snapshots, table properties.
- **Snapshot**: an immutable record of what files made up the table at a point in time. Writing new data creates a new snapshot; the old one is preserved (enabling time travel).
- **Manifest list**: points to manifest files for this snapshot.
- **Manifest file**: tracks individual data files with statistics (min/max values per column, record count, null count).

### Hidden partitioning

Traditional Hive partitioning requires you to include the partition column in your data and queries:

```sql
-- Hive: must filter on the physical partition column
SELECT * FROM orders WHERE dt = '2024-01-15'
```

If you forget `WHERE dt = ...`, Hive scans every partition (full table scan).

Iceberg's hidden partitioning separates the physical layout from the logical query:

```python
# Table is physically partitioned by month(created_at)
# but you query with any date predicate
spark.sql("SELECT * FROM orders WHERE created_at > '2024-01-01'")
# Iceberg automatically maps this to the correct month partitions
```

You define the partition transform once when creating the table; queries don't need to know about it.

### ACID transactions

Iceberg uses **optimistic concurrency control**. When two writers commit at the same time:

1. Both read the current metadata file
2. Both write their data files
3. Both try to atomically update the metadata pointer
4. One wins; the other detects a conflict and retries

The metadata pointer update is a single atomic rename operation (on S3, this is implemented differently — more on that below).

### Time travel

Every snapshot is preserved (until explicitly expired). You can query any past state:

```sql
-- Query as of a specific timestamp
SELECT * FROM orders TIMESTAMP AS OF '2024-01-15 10:00:00'

-- Query as of a specific snapshot ID
SELECT * FROM orders VERSION AS OF 12345678

-- See the history
SELECT * FROM orders.history
```

### Row-level deletes (Iceberg v2)

Iceberg v2 introduced **position delete files** and **equality delete files**:

- **Position delete**: records `(file_path, row_position)` pairs to logically mark rows as deleted without rewriting files
- **Equality delete**: records column values that identify rows to delete

This makes `UPDATE` and `DELETE` operations cheap on large tables. The actual file rewrite happens later during compaction (`rewrite_data_files`).

The `MERGE INTO` SQL (upsert) uses equality deletes under the hood when configured with `merge-on-read` mode.

---

## 4. IcebergWriter — implementation walkthrough

**File:** [killuhub/storage/iceberg/writer.py](../../killuhub/storage/iceberg/writer.py)

### Initialization

```python
class IcebergWriter(BaseStorageWriter):
    def __init__(self, spark: Any, **kwargs):
        self._spark = spark
        self.catalog_name = kwargs.get("catalog_name", "local")
        self.warehouse = kwargs.get("warehouse", "/tmp/killuhub-warehouse")
        self.partition_by: list[str] = kwargs.get("partition_by", [])
        self.merge_keys: list[str] = kwargs.get("merge_keys", [])
        self._schema_mgr = IcebergSchemaManager(spark, self.catalog_name)
```

The writer receives the `SparkSession` from the engine (via `Pipeline._build_writer()`). It needs the Spark session because all Iceberg operations go through Spark SQL.

### Append mode

```python
def _append(self, df, table_name):
    if not self.table_exists(table_name):
        self._schema_mgr.create_table(table_name, df, partition_by=self.partition_by)
    else:
        df.writeTo(table_name).append()
```

`df.writeTo(table_name)` is Spark's DSv2 (DataSource v2) API for Iceberg. It's more powerful than the old `.write.format("iceberg")` approach — supports `append()`, `overwritePartitions()`, and `createOrReplace()`.

If the table doesn't exist yet, `create_table()` is called to initialize it with the correct schema and partitioning.

### Merge mode (upsert)

```python
def _merge(self, df, table_name):
    df.createOrReplaceTempView("_killuhub_source")

    join_clause = " AND ".join(
        f"target.{k} = source.{k}" for k in self.merge_keys
    )
    set_clause = ", ".join(
        f"target.{c} = source.{c}" for c in non_key_cols
    )

    self._spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING _killuhub_source AS source
        ON {join_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT (...)  VALUES (...)
    """)
```

The `MERGE INTO` statement is standard SQL (available since SQL:2003). Iceberg's Spark extension enables it on Iceberg tables.

For a table with `merge_keys=["order_id"]`:
- If `order_id` exists in target → update all other columns
- If `order_id` does not exist → insert as a new row
- Net effect: idempotent writes — running the same batch twice gives the same result

This is essential for pipelines that might re-process data (restarts, retries, backfills).

---

## 5. IcebergSchemaManager — schema evolution and maintenance

**File:** [killuhub/storage/iceberg/schema_manager.py](../../killuhub/storage/iceberg/schema_manager.py)

### Schema evolution

One of Iceberg's strongest features is safe schema evolution. You can add, drop, or rename columns without rewriting any data files:

```python
schema_mgr = IcebergSchemaManager(spark, catalog_name="local")

# Add a new column (existing files simply return NULL for this column)
schema_mgr.add_column("local.shop.orders", "discount_pct", "DOUBLE")

# Rename a column (metadata-only change)
schema_mgr.rename_column("local.shop.orders", "amt", "amount")

# Drop a column (metadata-only; data is still in files but hidden from queries)
schema_mgr.drop_column("local.shop.orders", "legacy_field")
```

All of these are **metadata-only operations** — no Parquet files are rewritten. Old files still work because Iceberg tracks which schema version applies to which file.

### Table creation with properties

```python
schema_mgr.create_table(
    table_name="local.shop.orders",
    df=df,
    partition_by=["created_at"],
    properties={
        "format-version": "2",           # enables row-level deletes
        "write.delete.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
    }
)
```

`format-version: 2` is required for `MERGE INTO`. Version 1 tables only support appends and full overwrites.

### Table maintenance

```python
# Remove snapshots older than 7 days to reclaim storage
schema_mgr.expire_snapshots("local.shop.orders", older_than_ms=7_days_in_ms)

# Compact small files into larger ones for better query performance
schema_mgr.rewrite_data_files("local.shop.orders")
```

Over time, incremental writes create many small files (the "small files problem"). `rewrite_data_files` merges them into larger files, dramatically improving query performance for downstream tools like Trino or Spark SQL.

---

## 6. Delta Lake — overview and stub

**File:** [killuhub/storage/delta/writer.py](../../killuhub/storage/delta/writer.py)

Delta Lake was created by Databricks and open-sourced. It uses a **transaction log** (`_delta_log/`) stored alongside the data files:

```
s3://bucket/orders/
├── part-001.parquet
├── part-002.parquet
└── _delta_log/
    ├── 00000000000000000000.json   ← initial commit: ADD file
    ├── 00000000000000000001.json   ← second commit: ADD file
    └── 00000000000000000010.checkpoint.parquet  ← aggregated checkpoint
```

Each `.json` file in the log is a commit — it records what files were added or removed. Reading the table means replaying the log to find the current set of live files.

### Why it's a stub in KilluHub

Delta Lake's architecture is excellent, but it's more tightly coupled to Databricks. The `delta-spark` Python package has the same `MERGE INTO` support, but the catalog story is less open (Unity Catalog is Databricks-native).

The stub implements the full interface correctly:

```python
class DeltaWriter(BaseStorageWriter):
    def write(self, df, table_name, mode="append"):
        path = self._path(table_name)
        if mode in ("append", "overwrite"):
            df.write.format("delta").mode(mode).save(path)
        elif mode == "merge":
            DeltaTable.forPath(self._spark, path) \
                .alias("target") \
                .merge(df.alias("source"), join_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
```

Switching to Delta is one line:

```python
PipelineConfig(storage_writer_name="delta", ...)
```

---

## 7. Apache Hudi — overview and stub

**File:** [killuhub/storage/hudi/writer.py](../../killuhub/storage/hudi/writer.py)

Hudi was created by Uber for their need to do efficient upserts on a massive scale. Its key innovation is the **Hudi timeline** — an ordered log of all table operations.

### Table types

| Type              | Write behavior      | Read behavior       | Best for              |
|-------------------|---------------------|---------------------|-----------------------|
| Copy-on-Write (COW) | Rewrites files on update | Reads latest files | Read-heavy workloads |
| Merge-on-Read (MOR) | Writes delta files  | Merges on read      | Write-heavy workloads |

COW is simpler and gives faster reads. MOR has faster writes but slower reads (merging happens at query time).

### The stub

```python
class HudiWriter(BaseStorageWriter):
    def write(self, df, table_name, mode="append"):
        hudi_mode = "upsert" if mode == "merge" else mode
        options = {
            "hoodie.table.name": table_name.split(".")[-1],
            "hoodie.datasource.write.recordkey.field": self.record_key_field,
            "hoodie.datasource.write.precombine.field": self.precombine_field,
            "hoodie.datasource.write.operation": hudi_mode,
            ...
        }
        df.write.format("hudi").options(**options).save(self._path(table_name))
```

`precombine_field` is Hudi's way of resolving conflicts: if two records have the same `record_key_field`, the one with the higher `precombine_field` value wins (typically a timestamp or version number).

---

## 8. Format comparison deep-dive

### Metadata approach

| Format  | Metadata storage                    | Catalog dependency      |
|---------|-------------------------------------|-------------------------|
| Iceberg | Manifest files + metadata JSON      | Optional (file-based works) |
| Delta   | `_delta_log/` JSON files            | Optional                |
| Hudi    | `.hoodie/` timeline + index         | Optional                |

### Multi-engine support (the most important differentiator)

| Engine       | Iceberg | Delta     | Hudi      |
|--------------|---------|-----------|-----------|
| Spark        | ✅      | ✅        | ✅        |
| Flink        | ✅      | ✅ (limited) | ✅     |
| Trino/Presto | ✅      | ✅        | ✅        |
| Spark SQL    | ✅      | ✅        | ✅        |
| Snowflake    | ✅      | ❌        | ❌        |
| Dremio       | ✅      | ❌        | Partial   |
| StarRocks    | ✅      | ✅        | ✅        |
| Databricks   | ✅ (Unity) | ✅ (native) | ✅    |

Iceberg wins on engine breadth. If you want to query from Trino AND Spark AND Snowflake, Iceberg is the safest choice.

### Hidden partitioning

Only Iceberg has it. Delta and Hudi require you to physically include partition columns in your data and reference them in queries.

### Catalog support

| Catalog          | Iceberg | Delta  | Hudi   |
|------------------|---------|--------|--------|
| Hive Metastore   | ✅      | ✅     | ✅     |
| AWS Glue         | ✅      | ✅     | ✅     |
| Databricks Unity | ✅      | ✅     | Partial |
| Nessie/Polaris   | ✅      | ❌     | ❌     |
| REST catalog     | ✅      | ❌     | ❌     |

**Nessie** is a Git-like catalog for Iceberg: create branches, merge them, roll back — just like Git but for data. Delta and Hudi don't support this.

---

## 9. Why KilluHub chose Iceberg

The decision comes down to three things:

**1. Future-proof multi-engine**

KilluHub's roadmap includes Flink streaming, Trino for SQL analytics, and Databricks for ML. Iceberg is the only format that works natively with all of them. Delta requires Databricks for the best experience. Hudi lacks Snowflake and Nessie support.

**2. Open governance**

Iceberg is a true Apache project. Delta Lake is open source but Databricks-governed — the roadmap is controlled by one company. Iceberg has contributions from Apple, Netflix, Adobe, Cloudera, AWS, and many others.

**3. Databricks compatibility via Unity Catalog**

Databricks launched Iceberg support in Unity Catalog. This means KilluHub can write Iceberg tables today with a local Hadoop catalog, and seamlessly migrate them to Unity Catalog when the scale requires Databricks — with zero data format changes.

---

## 10. Swapping storage writers

All three writers implement `BaseStorageWriter`. Switching is one string:

```python
# Iceberg (default)
PipelineConfig(storage_writer_name="iceberg", ...)

# Delta Lake
PipelineConfig(storage_writer_name="delta", ...)

# Hudi
PipelineConfig(storage_writer_name="hudi", storage_kwargs={
    "record_key_field": "order_id",
    "precombine_field": "updated_at",
})
```

Each writer class is instantiated by `Pipeline._build_writer()` with the `storage_kwargs` as extra configuration. The Pipeline doesn't know which writer it's using — it just calls `.write(df, table_name, mode)`.
