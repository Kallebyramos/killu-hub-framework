# Processing Engines — Study Guide

The processing layer sits between the connectors and the storage layer. Its job is to convert raw Python dicts into a distributed DataFrame, apply transformations, and hand the result to the storage writer.

KilluHub ships two engines: **Spark** (batch-first) and **Flink** (streaming-first). Both implement the same `BaseEngine` interface, so switching between them is a configuration change.

---

## Table of contents

1. [Why a processing engine?](#1-why-a-processing-engine)
2. [Spark vs Flink — when to use each](#2-spark-vs-flink--when-to-use-each)
3. [Apache Spark — architecture overview](#3-apache-spark--architecture-overview)
4. [SparkEngine — deep dive](#4-sparkengine--deep-dive)
5. [Apache Flink — architecture overview](#5-apache-flink--architecture-overview)
6. [FlinkEngine — deep dive](#6-flinkengine--deep-dive)
7. [Transformations — how they work](#7-transformations--how-they-work)
8. [Iceberg catalog wiring](#8-iceberg-catalog-wiring)
9. [Choosing an engine in practice](#9-choosing-an-engine-in-practice)

---

## 1. Why a processing engine?

You could write ingestion code that reads from Postgres and writes directly to files. But a distributed processing engine gives you things raw Python cannot:

- **Scale**: process 10GB or 10TB with the same code, just give it more workers
- **Columnar operations**: filter, aggregate, join millions of rows efficiently without loops
- **Fault tolerance**: if one worker crashes, Spark/Flink restarts just that task
- **Schema inference**: automatically figures out data types from your records
- **Native Iceberg integration**: Spark and Flink have first-class Iceberg support — `MERGE INTO`, schema evolution, and hidden partitioning work out of the box

---

## 2. Spark vs Flink — when to use each

| Aspect             | Spark                              | Flink                               |
|--------------------|------------------------------------|-------------------------------------|
| **Primary model**  | Micro-batch (batch-first)          | True streaming (event-by-event)     |
| **Latency**        | Seconds to minutes                 | Milliseconds to seconds             |
| **Use case**       | ETL, nightly jobs, large batch loads | Real-time pipelines, event streaming |
| **State management** | Limited (stateless by default)   | First-class — windowing, joins      |
| **Maturity**       | Very mature, huge ecosystem        | Maturing fast, strong in Europe     |
| **Databricks**     | Native                             | External cluster needed             |
| **Kafka → Iceberg**| Good (structured streaming)        | Excellent (native Iceberg sink)     |

**Rule of thumb for KilluHub:**
- Scheduled batch jobs (Postgres, S3, REST API → Iceberg): **Spark**
- Real-time Kafka pipelines with low latency requirements: **Flink**
- Databricks target: **Spark** (it's the native engine)

---

## 3. Apache Spark — architecture overview

```
┌─────────────────────────────────────────────────┐
│                  Driver Process                  │
│  SparkContext  →  DAG Scheduler  →  Task Scheduler│
└─────────────────────┬───────────────────────────┘
                      │  distributes tasks
          ┌───────────┼───────────┐
          ▼           ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ Executor │ │ Executor │ │ Executor │
    │ (Worker) │ │ (Worker) │ │ (Worker) │
    └──────────┘ └──────────┘ └──────────┘
```

- **Driver**: your Python program. It creates the DAG (Directed Acyclic Graph) of operations but doesn't process data itself.
- **Executors**: JVM processes on worker nodes. They do the actual work — reading, filtering, writing.
- **DAG**: Spark is lazy. When you write `df.filter(...).groupBy(...)`, nothing runs yet. Only when you call an **action** (`.write()`, `.collect()`, `.show()`) does Spark compile the DAG and execute it.

### SparkSession

The `SparkSession` is the entry point to Spark. In local mode (development), it starts everything in one process:

```python
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
```

`local[*]` means: run locally, use all available CPU cores. In production, you'd point it to a cluster master.

### DataFrames

A Spark DataFrame is a distributed, immutable table of rows. It has a schema (column names and types) inferred from your data. All operations are lazy and produce new DataFrames:

```python
df = spark.createDataFrame([{"id": 1, "amount": 99.0}])
# Nothing has happened yet

df2 = df.filter("amount > 50")    # still lazy
df3 = df2.withColumn("tax", df2.amount * 0.1)  # still lazy

df3.write.format("iceberg").save(...)  # NOW Spark executes the full plan
```

---

## 4. SparkEngine — deep dive

**File:** [killuhub/processing/spark_engine.py](../../killuhub/processing/spark_engine.py)

### Session initialization

```python
def init_session(self, app_name: str = "KilluHub", **kwargs) -> None:
    builder = SparkSession.builder.appName(app_name)

    # Iceberg catalog wiring
    catalog_name = kwargs.get("catalog_name", "local")
    warehouse = kwargs.get("warehouse", "/tmp/killuhub-warehouse")

    builder = (
        builder
        .config("spark.jars.packages", iceberg_impl)
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )

    self._spark = builder.getOrCreate()
```

Several important things happening here:

**`spark.jars.packages`** — tells Spark to download the Iceberg runtime JAR from Maven at session start. This is how Spark gets the `org.apache.iceberg.*` classes without you manually downloading JARs.

**Catalog registration** — tells Spark that any table name starting with `local.` should be resolved through the Iceberg catalog backed by a local Hadoop filesystem. In production, you'd swap `"hadoop"` for `"hive"` or `"rest"` (Nessie/Polaris).

**Extensions** — enables Iceberg-specific SQL syntax like `MERGE INTO` and `CALL system.rewrite_data_files(...)`.

### `getOrCreate()`

If a SparkSession already exists in the JVM process, `getOrCreate()` returns it. This prevents accidentally starting two sessions, which would fail with a port conflict.

### Creating a DataFrame from records

```python
def create_dataframe(self, records: list[dict[str, Any]]) -> Any:
    return self._spark.createDataFrame(records)
```

Spark infers the schema from the first record. For example:

```python
records = [
    {"id": 1, "name": "Alice", "amount": 99.0},
    {"id": 2, "name": "Bob",   "amount": 149.5},
]
df = spark.createDataFrame(records)
# Schema: id LongType, name StringType, amount DoubleType
```

### Exposing the raw session

```python
@property
def spark(self) -> Any:
    return self._spark
```

Advanced users can access `engine.spark` directly to run arbitrary SQL, read other tables, or use Spark ML — without being constrained by the KilluHub interface.

---

## 5. Apache Flink — architecture overview

```
┌─────────────────────────────────────────────────┐
│              JobManager (master)                 │
│   Job scheduling, checkpointing, coordination    │
└─────────────────────┬───────────────────────────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │TaskManager│ │TaskManager│ │TaskManager│
    │ (slot)   │ │ (slot)   │ │ (slot)   │
    └──────────┘ └──────────┘ └──────────┘
```

Flink's key differentiator is **stateful stream processing**:

- **Checkpoints**: Flink periodically saves the state of all operators to durable storage. If a worker fails, it restores from the last checkpoint — exactly-once processing guarantee.
- **Watermarks**: Flink handles out-of-order events. You can define that "events arriving up to 5 seconds late are still acceptable".
- **Windows**: group events by time (tumbling 1-minute windows, sliding 5-minute windows) or by count.

### Table API vs DataStream API

Flink has two APIs:

- **DataStream API** — low-level, full control over streams, state, and time
- **Table API / SQL** — higher-level, SQL-like, closer to what KilluHub uses

KilluHub's `FlinkEngine` uses the **Table API** because it aligns naturally with the connector-to-storage model and has native Iceberg sink support.

---

## 6. FlinkEngine — deep dive

**File:** [killuhub/processing/flink_engine.py](../../killuhub/processing/flink_engine.py)

### Session initialization

```python
def init_session(self, app_name: str = "KilluHub-Flink", **kwargs) -> None:
    mode = kwargs.get("mode", "streaming")
    if mode == "batch":
        settings = EnvironmentSettings.in_batch_mode()
    else:
        settings = EnvironmentSettings.in_streaming_mode()

    self._t_env = TableEnvironment.create(settings)

    # Register Iceberg catalog
    self._t_env.execute_sql(f"""
        CREATE CATALOG iceberg_catalog WITH (
            'type'='iceberg',
            'catalog-type'='hadoop',
            'warehouse'='{warehouse}'
        )
    """)
```

Flink can run in **batch mode** (process a finite dataset completely) or **streaming mode** (run indefinitely, processing events as they arrive). For Kafka → Iceberg pipelines, streaming mode is the right choice.

### Pandas bridge for batch records

```python
def create_dataframe(self, records: list[dict[str, Any]]) -> Any:
    import pandas as pd
    df = pd.DataFrame(records)
    return self._t_env.from_pandas(df)
```

For batch records coming from connectors, we use pandas as an intermediary bridge. In a full streaming pipeline, you'd define a Flink source connector (Kafka source table) instead.

### Streaming Kafka → Iceberg (extended usage)

For real streaming, you'd bypass `create_dataframe()` and define Flink source/sink tables directly:

```python
engine = FlinkEngine()
engine.init_session(mode="streaming")
t_env = engine.table_env

t_env.execute_sql("""
    CREATE TABLE kafka_source (
        event_id STRING,
        user_id BIGINT,
        action STRING,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'user-events',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json'
    )
""")

t_env.execute_sql("""
    CREATE TABLE iceberg_sink (
        event_id STRING,
        user_id BIGINT,
        action STRING,
        event_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'iceberg_catalog',
        'database-name' = 'events',
        'table-name' = 'user_actions'
    )
""")

t_env.execute_sql("""
    INSERT INTO iceberg_sink SELECT * FROM kafka_source
""")
```

This is the eventual direction for KilluHub's Flink integration.

---

## 7. Transformations — how they work

The `transform_fn` is a callable that takes a DataFrame and returns a DataFrame:

```python
# For Spark
def my_transform(df):
    from pyspark.sql import functions as F
    return (
        df
        .filter(F.col("amount") > 0)
        .withColumn("amount_brl", F.col("amount") * 5.0)
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["order_id"])
    )

config = PipelineConfig(..., transform_fn=my_transform)
```

The engine applies it via:

```python
def apply_transform(self, df, transform_fn):
    if transform_fn is None:
        return df
    return transform_fn(df)
```

This design gives you full access to the engine's native API. You're not limited to what KilluHub defines — you can use any Spark or Flink feature.

### Common Spark transform patterns

```python
from pyspark.sql import functions as F

# Cast types
df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))

# Parse date strings
df.withColumn("created_date", F.to_date("created_at", "yyyy-MM-dd'T'HH:mm:ss"))

# Add partition column
df.withColumn("year_month", F.date_format("created_at", "yyyy-MM"))

# Filter and deduplicate
df.filter("status = 'active'").dropDuplicates(["user_id"])

# Window function — rank by recency
from pyspark.sql.window import Window
w = Window.partitionBy("user_id").orderBy(F.desc("updated_at"))
df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

---

## 8. Iceberg catalog wiring

Both engines need to know where the Iceberg catalog is. The `engine_kwargs` in `PipelineConfig` are passed directly to `init_session()`:

```python
engine_kwargs={
    "catalog_name": "prod",
    "catalog_type": "hive",          # "hadoop" for local, "hive" for Hive Metastore, "rest" for Nessie
    "warehouse": "s3://my-bucket/warehouse",
    "extra_config": {
        "spark.hadoop.fs.s3a.access.key": "...",
        "spark.hadoop.fs.s3a.secret.key": "...",
    }
}
```

### Catalog types

| Type       | Backed by                        | Best for                          |
|------------|----------------------------------|-----------------------------------|
| `hadoop`   | Plain filesystem (S3, HDFS, local) | Development, simple setups      |
| `hive`     | Hive Metastore                   | On-prem Hadoop environments       |
| `rest`     | Nessie or Polaris (REST catalog) | Multi-engine, Git-like branches   |
| Glue       | AWS Glue Data Catalog            | AWS-native setups                 |
| Unity      | Databricks Unity Catalog         | Databricks target                 |

---

## 9. Choosing an engine in practice

**Use Spark when:**
- You're running scheduled batch pipelines (hourly, daily)
- Source is Postgres, S3, or REST API
- Target is Databricks
- Team knows SQL and PySpark

**Use Flink when:**
- You need sub-second latency (real-time dashboards, fraud detection)
- Source is Kafka or another event stream
- You need stateful processing (aggregations over time windows)
- You want true streaming (not micro-batch)

**The KilluHub advantage:** you can start with Spark today and migrate to Flink later by changing one field in your `PipelineConfig`. The connector and storage layers don't change at all.
