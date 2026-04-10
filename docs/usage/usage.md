# KilluHub — How to Use
## End-to-end guide for every execution scenario

This document is your practical reference. Each section is self-contained — jump to the scenario that matches what you're trying to do.

---

## Table of contents

1. [Mental model — how all pieces fit](#1-mental-model--how-all-pieces-fit)
2. [The config file — the one thing you fill in](#2-the-config-file--the-one-thing-you-fill-in)
3. [Chain pipeline — Bronze + Silver in one file](#3-chain-pipeline--bronze--silver-in-one-file)
4. [Batch Bronze — PostgreSQL → Iceberg](#4-batch-bronze--postgresql--iceberg)
5. [Batch Bronze — MySQL → Iceberg](#5-batch-bronze--mysql--iceberg)
6. [Batch Bronze — REST API → Iceberg](#6-batch-bronze--rest-api--iceberg)
7. [Streaming Bronze — Kafka → Iceberg](#7-streaming-bronze--kafka--iceberg)
8. [Batch Silver — Bronze → Silver](#8-batch-silver--bronze--silver)
9. [Running locally](#9-running-locally)
10. [Running on Databricks (Community or full)](#10-running-on-databricks-community-or-full)
11. [Running on EKS (Spark Operator)](#11-running-on-eks-spark-operator)
12. [Running on AWS EMR](#12-running-on-aws-emr)
13. [Full datalake walkthrough — Postgres → Bronze → Silver on Databricks](#13-full-datalake-walkthrough--postgres--bronze--silver-on-databricks)
14. [Full datalake walkthrough — Kafka streaming → Bronze on EKS](#14-full-datalake-walkthrough--kafka-streaming--bronze-on-eks)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Mental model — how all pieces fit

```
You fill in one file:
┌──────────────────────────────────────────────────────────────┐
│  config.yaml (or values.yaml for Helm)                       │
│                                                              │
│  type:     bronze | silver | chain                           │
│  mode:     batch  | streaming  (per stage in chain)          │
│  platform: (set in Helm values only)                         │
│                                                              │
│  connector:  where data comes from (postgres/mysql/kafka…)   │
│  batch:      strategy + watermark (if mode: batch)           │
│  streaming:  trigger + checkpoint (if mode: streaming)       │
│  bronze/silver: target table, partition columns              │
│  contract:   quality rules enforced before write             │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
     main.py --config your-config.yaml
           │
           ├── type: chain     → run stages in order (bronze → silver)
           ├── mode: batch    → BronzePipeline or SilverPipeline
           └── mode: streaming → StreamingBronzePipeline
                     │
                     ▼
           connector (reads from source)
                     │
                     ▼
           SparkEngine (detects environment automatically)
                     │
              ┌──────┴─────────────────────────┐
              │ LOCAL     → local[*]            │
              │ DATABRICKS → getOrCreate()      │
              │ EKS       → Spark Operator      │
              │ EMR       → YARN                │
              └────────────────────────────────┘
                     │
                     ▼
           ContractValidator (optional quality gate)
                     │
                     ▼
           IcebergWriter → Bronze or Silver Iceberg table
```

The only thing that changes between platforms is **how SparkEngine starts the session**. Your config YAML, connector, and pipeline logic are identical everywhere.

---

## 2. The config file — the one thing you fill in

Every pipeline needs exactly one YAML config. Here is the full schema with every possible key:

```yaml
# ── Required ─────────────────────────────────────────────────────────────────
type: bronze        # bronze | silver
mode: batch         # batch  | streaming

# ── Batch config (required when mode: batch) ─────────────────────────────────
batch:
  strategy: incremental     # full | incremental
  watermark_column: updated_at
  initial_watermark: "2024-01-01T00:00:00"

# ── Streaming config (required when mode: streaming) ─────────────────────────
streaming:
  trigger: processingTime           # processingTime | once | availableNow
  trigger_interval: "30 seconds"
  checkpoint_location: "/tmp/checkpoints"
  output_mode: append               # append | update | complete

# ── Connector (required when type: bronze) ───────────────────────────────────
connector:
  name: postgres    # postgres | mysql | kafka | rest_api
  config: { ... }   # connector-specific keys (see each section below)
  # For streaming Kafka:
  stream_format: kafka
  stream_options: { ... }

# ── Engine (always required) ─────────────────────────────────────────────────
engine:
  name: spark
  warehouse: /tmp/killuhub-warehouse   # S3 / DBFS / local path
  catalog_name: local                  # Iceberg catalog name
  catalog_type: hadoop                 # hadoop | rest | unity | glue
  unity_catalog: main                  # Databricks only

# ── Bronze layer (required when type: bronze) ─────────────────────────────────
bronze:
  table: local.bronze.orders           # catalog.schema.table
  source_name: postgres.shop.orders    # human label for lineage
  partition_by: [_ingestion_date]      # Iceberg partition columns

# ── Silver layer (required when type: silver) ─────────────────────────────────
silver:
  bronze_table: local.bronze.orders
  silver_table: local.silver.orders
  key_columns: [order_id]              # deduplication keys
  date_columns: [created_at]           # columns to parse as timestamps
  date_dimension_column: created_at    # column to derive year/month/day from
  type_map: { amount: double }         # cast these columns
  null_check_columns: [order_id]       # fail if null
  partition_by: [created_date]
  state_store: json                    # json | iceberg

# ── Contract (optional — recommended) ────────────────────────────────────────
contract:
  on_violation: warn      # warn | fail
  min_row_count: 1
  columns:
    - name: order_id
      type: long
      nullable: false
      min_value: 1
```

**Environment variable substitution** works everywhere in the file:
```yaml
password: ${PG_PASSWORD}          # required, error if not set
warehouse: ${WAREHOUSE:-/tmp/wh}  # optional, falls back to /tmp/wh
```

---

## 3. Chain pipeline — Bronze + Silver in one file

The **chain** type is the recommended way to run a full medallion pipeline. One config file, one command — bronze runs first, silver runs second. The `engine:` block is defined once at the top and shared by all stages.

**Key feature: `silver.bronze_table` is auto-injected.** You write the table name once (in `bronze.table`) and the framework automatically passes it as the source to the silver stage. No repetition.

```yaml
type: chain

# Shared engine — inherited by all stages (each stage can override if needed)
engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

stages:

  - name: bronze-orders
    type: bronze
    mode: batch
    batch:
      strategy: incremental
      watermark_column: updated_at
      initial_watermark: "2024-01-01T00:00:00"
    connector:
      name: postgres
      config:
        host: ${PG_HOST:-localhost}
        database: shop
        user: postgres
        password: ${PG_PASSWORD}
        query: "SELECT * FROM orders WHERE updated_at > '${WATERMARK}'"
    bronze:
      table: ${CATALOG:-local}.bronze.orders
      source_name: postgres.shop.orders
      partition_by: [_ingestion_date]

  - name: silver-orders
    type: silver
    mode: batch
    batch:
      strategy: incremental
      watermark_column: _ingested_at
      initial_watermark: "1970-01-01T00:00:00"
    silver:
      # bronze_table is auto-injected from the bronze stage above — no need to repeat it
      silver_table: ${CATALOG:-local}.silver.orders
      key_columns: [order_id]
      date_columns: [created_at, updated_at]
      date_dimension_column: created_at
      type_map: { amount: double, quantity: int }
      null_check_columns: [order_id, customer_id]
      drop_columns: [_batch_id]
      partition_by: [created_date]
      state_store: json
    contract:
      on_violation: fail
      min_row_count: 0
      columns:
        - { name: order_id, type: long, nullable: false }
        - { name: amount, type: double, nullable: false, min_value: 0 }
        - { name: created_at, type: timestamp, nullable: false }
```

```bash
python main.py --config config/chain_orders.yaml

# Validate config without running
python main.py --config config/chain_orders.yaml --dry-run
```

### When each strategy makes sense

| Scenario | `batch.strategy` |
|----------|-----------------|
| First ever run — load all historical data | `full` |
| Daily/hourly scheduled runs | `incremental` |
| Reprocessing after schema change | `full` |
| Normal production operation | `incremental` |

Both bronze and silver support `full` and `incremental` independently. You can do `bronze: full` + `silver: incremental` to reload the raw data but only process new silver rows.

---

## 4. Batch Bronze — PostgreSQL → Iceberg

**What it does:** reads rows from a Postgres table in batches, stamps metadata columns, validates the contract, and appends to an Iceberg table.

### Config

```yaml
type: bronze
mode: batch

batch:
  strategy: incremental      # only reads rows updated since last run
  watermark_column: updated_at
  initial_watermark: "2024-01-01T00:00:00"

connector:
  name: postgres
  config:
    host: ${PG_HOST}
    port: ${PG_PORT:-5432}
    database: ${PG_DATABASE}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    # Write exactly the query you want — KilluHub runs it as-is
    query: >
      SELECT order_id, customer_id, amount, status, created_at, updated_at
      FROM orders
      WHERE updated_at > '${WATERMARK:-1970-01-01}'
    batch_size: 10000          # rows per server-side cursor fetch

engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

bronze:
  table: ${CATALOG:-local}.bronze.orders
  source_name: postgres.shop.orders
  partition_by: [_ingestion_date]

contract:
  on_violation: warn
  min_row_count: 1
  columns:
    - name: order_id
      type: long
      nullable: false
    - name: amount
      type: double
      nullable: false
      min_value: 0
```

### Run

```bash
# Set credentials
export PG_HOST=localhost PG_DATABASE=shop PG_USER=admin PG_PASSWORD=secret

# Run
python main.py --config config/bronze_postgres.yaml

# Verify config without running
python main.py --config config/bronze_postgres.yaml --dry-run
```

### What gets written

Every source row gets these extra columns appended automatically:

| Column | Example value |
|--------|--------------|
| `_ingested_at` | `2024-07-15 10:23:45` |
| `_source_name` | `postgres.shop.orders` |
| `_batch_id` | `a1b2c3d4-…` (UUID) |
| `_batch_mode` | `incremental` |
| `_ingestion_date` | `2024-07-15` (partition) |

### Incremental vs full

| `strategy` | Behaviour |
|-----------|-----------|
| `full` | Runs the query as-is every time, writes all rows |
| `incremental` | Tracks a watermark cursor. On the first run uses `initial_watermark`. After each run, saves `max(watermark_column)` to a state file. Next run starts from there. |

State is saved to `.killuhub_state/` next to your config file (or to an Iceberg table when `state_store: iceberg`).

---

## 5. Batch Bronze — MySQL → Iceberg

Almost identical to Postgres. The only difference is the connector name and the optional TLS fields.

```yaml
type: bronze
mode: batch

batch:
  strategy: incremental
  watermark_column: updated_at
  initial_watermark: "2024-01-01T00:00:00"

connector:
  name: mysql
  config:
    host: ${MYSQL_HOST}
    port: ${MYSQL_PORT:-3306}
    database: ${MYSQL_DATABASE}
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    query: >
      SELECT order_id, customer_id, amount, status, updated_at
      FROM orders
    batch_size: 10000
    # TLS — uncomment for RDS or managed MySQL
    # ssl_ca: /etc/ssl/certs/ca-bundle.crt

engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

bronze:
  table: ${CATALOG:-local}.bronze.orders
  source_name: mysql.shop.orders
  partition_by: [_ingestion_date]
```

```bash
export MYSQL_HOST=localhost MYSQL_DATABASE=shop MYSQL_USER=root MYSQL_PASSWORD=secret
python main.py --config config/bronze_mysql.yaml
```

---

## 6. Batch Bronze — REST API → Iceberg

Reads from a paginated REST API. Supports four pagination strategies.

```yaml
type: bronze
mode: batch

batch:
  strategy: incremental
  watermark_column: created_at
  initial_watermark: "2024-01-01T00:00:00"

connector:
  name: rest_api
  config:
    base_url: https://api.example.com
    endpoint: /v1/orders
    auth_type: bearer                  # bearer | basic | none
    token: ${API_TOKEN}

    # Pagination strategy — pick one:
    pagination: page                   # none | page | cursor | offset
    page_param: page
    page_size_param: per_page
    page_size: 100
    # cursor pagination:
    # pagination: cursor
    # cursor_param: next_cursor
    # cursor_path: meta.next_cursor
    # offset pagination:
    # pagination: offset
    # offset_param: offset
    # limit_param: limit

engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

bronze:
  table: ${CATALOG:-local}.bronze.api_orders
  source_name: api.example.orders
  partition_by: [_ingestion_date]
```

```bash
export API_TOKEN=my-token
python main.py --config config/bronze_api.yaml
```

---

## 7. Streaming Bronze — Kafka → Iceberg

**What it does:** runs continuously, reading from a Kafka topic in microbatches and appending to an Iceberg table. Uses Spark Structured Streaming — the job never exits until you cancel it.

```yaml
type: bronze
mode: streaming      # ← this is the key change

streaming:
  trigger: processingTime          # run a microbatch every 30s
  trigger_interval: "30 seconds"
  checkpoint_location: ${CHECKPOINT_PATH:-/tmp/killuhub-checkpoints/orders}
  output_mode: append

connector:
  name: kafka
  stream_format: kafka
  stream_options:
    kafka.bootstrap.servers: ${KAFKA_BROKERS:-localhost:9092}
    subscribe: ${KAFKA_TOPIC:-orders}
    startingOffsets: latest          # latest = only new messages
    failOnDataLoss: "false"

engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

bronze:
  table: ${CATALOG:-local}.bronze.orders_stream
  source_name: kafka.orders
  partition_by: [_ingestion_date]

contract:
  on_violation: warn
  min_row_count: 0                   # 0 = empty microbatches are fine
  columns:
    - name: value                    # Kafka raw value column
      nullable: false
```

```bash
export KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=orders CHECKPOINT_PATH=/tmp/ckpt
python main.py --config config/bronze_kafka.yaml
# Runs forever — cancel with Ctrl+C
```

### Trigger options explained

| Trigger | When to use |
|---------|-------------|
| `processingTime` + interval | Normal streaming — processes continuously every N seconds |
| `once` | Run exactly one microbatch, process all pending data, then exit. Good for scheduled jobs that want streaming semantics without a long-running process |
| `availableNow` | Like `once` but respects rate limits. Best for catching up on backfill |
| `continuous` | Experimental low-latency mode. Millisecond latency but limited operations |

### What the Kafka schema looks like in Bronze

Spark Structured Streaming reads Kafka into a fixed schema:

| Column | Type | Notes |
|--------|------|-------|
| `key` | binary | Kafka message key |
| `value` | binary | Raw message bytes — parse with a transform |
| `topic` | string | |
| `partition` | int | |
| `offset` | long | |
| `timestamp` | timestamp | Kafka event timestamp |

To parse JSON values, add a `value_deserializer` in code (see [docs/layers/layers.md](../layers/layers.md)).

---

## 8. Batch Silver — Bronze → Silver

**What it does:** reads from a Bronze Iceberg table, applies transformations (type casting, date parsing, deduplication), validates a contract, and writes to a Silver Iceberg table.

Silver always runs as `mode: batch` — it reads from Iceberg, which is already durable and ordered.

```yaml
type: silver
mode: batch

batch:
  strategy: incremental          # only processes Bronze rows added since last run
  watermark_column: _ingested_at # filter on Bronze metadata column
  initial_watermark: "1970-01-01T00:00:00"

engine:
  name: spark
  warehouse: ${WAREHOUSE:-/tmp/killuhub-warehouse}
  catalog_name: ${CATALOG:-local}
  catalog_type: hadoop

silver:
  bronze_table: ${CATALOG:-local}.bronze.orders
  silver_table: ${CATALOG:-local}.silver.orders

  # Deduplication — keep the latest row per key_columns
  key_columns: [order_id]

  # Parse these string columns into TimestampType
  date_columns: [created_at, updated_at]

  # Derive year/month/day/hour/date from this column
  date_dimension_column: created_at

  # Cast these columns explicitly
  type_map:
    amount: double
    quantity: int

  # Drop rows where these are null (before writing)
  null_check_columns: [order_id, customer_id]

  # Drop columns you don't want in Silver
  drop_columns: [_batch_id]

  # Partition Silver table by this derived column
  partition_by: [created_date]

  # State store — tracks the watermark cursor between runs
  state_store: json              # json (dev) | iceberg (prod)

contract:
  on_violation: fail             # fail = abort the pipeline on any violation
  min_row_count: 0
  columns:
    - name: order_id
      type: long
      nullable: false
    - name: amount
      type: double
      nullable: false
      min_value: 0
    - name: created_at
      type: timestamp
      nullable: false
    - name: created_year
      type: integer
      nullable: false
```

```bash
python main.py --config config/silver_orders.yaml
```

### What transformations Silver applies (in order)

1. **Filter by watermark** — reads only Bronze rows added since last run
2. **Parse date columns** — converts string → `TimestampType`
3. **Add date dimensions** — derives `created_year`, `created_month`, `created_day`, `created_hour`, `created_date`
4. **Cast types** — per `type_map`
5. **Rename columns** — per `rename_map` (if set)
6. **Drop columns** — per `drop_columns`
7. **Filter nulls** — drops rows where `null_check_columns` are null
8. **Deduplicate** — window function, keeps latest row per `key_columns`
9. **Add metadata** — stamps `_processed_at`
10. **Contract validation** — validates against `contract`
11. **Write** — INCREMENTAL → append; FULL → overwrite partitions

### State store: json vs iceberg

| `state_store` | When to use | Where state is saved |
|--------------|-------------|---------------------|
| `json` | Local dev and testing | `.killuhub_state/` directory |
| `iceberg` | Production — durable, survives pod restarts | Iceberg table in the same catalog |

---

## 9. Running locally

Local mode requires no cluster. Spark runs in `local[*]` mode using all CPU cores on your machine.

### Setup

```bash
cd /path/to/killu-hub-framework

# Install the framework + Spark
pip install -e ".[spark,postgres,kafka]"
# Or install everything:
pip install -e ".[all]"

# Java is required for Spark
java -version   # must be Java 11 or 17
# Install Java if missing:
# Ubuntu: sudo apt install openjdk-17-jdk
# Mac:    brew install openjdk@17
```

### Run a batch Bronze pipeline

```bash
# Set env vars for your source
export PG_HOST=localhost PG_DATABASE=shop PG_USER=admin PG_PASSWORD=secret

# Run
python main.py --config config/bronze_postgres.yaml

# Inspect the result
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]') \
    .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config('spark.sql.catalog.local.warehouse', '/tmp/killuhub-warehouse') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .getOrCreate()
spark.sql('SELECT * FROM local.bronze.orders LIMIT 5').show()
"
```

### Run a dry-run (validates config without executing)

```bash
python main.py --config config/bronze_postgres.yaml --dry-run
```

### Force a specific environment (useful for testing)

```bash
# Simulate Databricks code path locally (won't actually use Databricks)
KILLUHUB_ENV=databricks python main.py --config config/bronze_postgres.yaml --dry-run
```

---

## 10. Running on Databricks (Community or full)

### Part A — Setup (one time)

**1. Build the wheel on your laptop:**
```bash
cd /path/to/killu-hub-framework
pip install build
python -m build --wheel
# Output: dist/killuhub-0.1.0-py3-none-any.whl
```

**2. Upload to Databricks:**
- Databricks UI → **Workspace** → your folder → **Import** → upload the `.whl`

**3. Create a cluster:**
- **Compute → Create Cluster**
- Runtime: `14.3 LTS` (Spark 3.5, Python 3.11)
- Under **Libraries**: Install New → Python Whl → select `killuhub-0.1.0-py3-none-any.whl`
- Under **Libraries**: also add `psycopg2-binary` (or whichever connector you need)

**4. Set secrets** (recommended over hardcoding):
- Go to **Settings → Secret Management** → create a scope named `killuhub`
- Add secrets: `pg-host`, `pg-password`, etc.
- In the cluster **Spark config** or notebook, expose them:
  ```
  spark.env.PG_HOST {{secrets/killuhub/pg-host}}
  spark.env.PG_PASSWORD {{secrets/killuhub/pg-password}}
  ```

### Part B — Run Bronze (notebook)

Open a notebook, attach to the cluster, and run:

```python
# Cell 1 — write config to DBFS
config = """
type: bronze
mode: batch

batch:
  strategy: incremental
  watermark_column: updated_at
  initial_watermark: "2024-01-01T00:00:00"

connector:
  name: postgres
  config:
    host: ${PG_HOST}
    port: 5432
    database: shop
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    query: "SELECT order_id, customer_id, amount, status, updated_at FROM orders"
    batch_size: 10000

engine:
  name: spark
  warehouse: /dbfs/killuhub/warehouse
  catalog_name: main
  catalog_type: unity
  unity_catalog: main

bronze:
  table: main.bronze.orders
  source_name: postgres.shop.orders
  partition_by: [_ingestion_date]

contract:
  on_violation: warn
  columns:
    - name: order_id
      type: long
      nullable: false
"""

import os
os.makedirs("/dbfs/killuhub/configs", exist_ok=True)
with open("/dbfs/killuhub/configs/bronze_orders.yaml", "w") as f:
    f.write(config)

print("Config written.")
```

```python
# Cell 2 — run the pipeline
import sys
sys.argv = ["main", "--config", "/dbfs/killuhub/configs/bronze_orders.yaml"]

from main import load_config, run

cfg = load_config("/dbfs/killuhub/configs/bronze_orders.yaml")
result = run(cfg)
print("Done:", result)
```

```python
# Cell 3 — inspect the Bronze table
spark.sql("SELECT * FROM main.bronze.orders LIMIT 10").show()
spark.sql("SELECT count(*) as total, _batch_mode, _source_name FROM main.bronze.orders GROUP BY 2,3").show()
```

### Part C — Run Silver (notebook)

```python
# Cell 1 — write Silver config
silver_config = """
type: silver
mode: batch

batch:
  strategy: incremental
  watermark_column: _ingested_at
  initial_watermark: "1970-01-01T00:00:00"

engine:
  name: spark
  warehouse: /dbfs/killuhub/warehouse
  catalog_name: main
  catalog_type: unity
  unity_catalog: main

silver:
  bronze_table: main.bronze.orders
  silver_table: main.silver.orders
  key_columns: [order_id]
  date_columns: [created_at, updated_at]
  date_dimension_column: created_at
  type_map:
    amount: double
  null_check_columns: [order_id, customer_id]
  partition_by: [created_date]
  state_store: iceberg

contract:
  on_violation: fail
  columns:
    - name: order_id
      type: long
      nullable: false
    - name: amount
      type: double
      nullable: false
      min_value: 0
"""

with open("/dbfs/killuhub/configs/silver_orders.yaml", "w") as f:
    f.write(silver_config)
```

```python
# Cell 2 — run Silver
sys.argv = ["main", "--config", "/dbfs/killuhub/configs/silver_orders.yaml"]

from main import load_config, run
cfg = load_config("/dbfs/killuhub/configs/silver_orders.yaml")
result = run(cfg)
print("Done:", result)
```

```python
# Cell 3 — inspect Silver
spark.sql("SELECT * FROM main.silver.orders LIMIT 10").show()
# Time travel — see previous snapshot
spark.sql("SELECT * FROM main.silver.orders VERSION AS OF 1 LIMIT 10").show()
```

### Part D — Schedule the job via Databricks Jobs API

Use the Helm chart to generate the job definition:

```bash
# On your laptop
helm template killuhub-bronze ./helm/killuhub \
  --set platform=databricks \
  --set pipeline.type=bronze \
  --set pipeline.connector.name=postgres \
  --set databricks.catalogName=main \
  > /tmp/bundle.yaml

# Deploy
databricks bundle deploy
databricks bundle run killuhub-job
```

Or use the Workflows UI: **Workflows → Create Job → Python Wheel → package: killuhub, entry point: main, parameters: --config /dbfs/killuhub/configs/bronze_orders.yaml**

### What happens automatically on Databricks (you don't configure these)

- `SparkSession.getOrCreate()` — reuses the cluster session, no new session created
- No `spark.stop()` call — would kill other jobs on the shared cluster
- No JAR packages configured — Databricks 14.x has Iceberg built in
- `USE CATALOG main` — issued automatically when `catalog_type: unity`

---

## 11. Running on EKS (Spark Operator)

### Prerequisites

- EKS cluster with Spark Operator installed (`helm install spark-operator …`)
- Karpenter with two NodePools: `spark-driver` (on-demand) and `spark-executor` (spot)
- IRSA role with S3 + Glue permissions
- Docker image built and pushed to ECR

### Part A — Build and push the Docker image

```bash
cd /path/to/killu-hub-framework

# Build
docker build -t killuhub:0.1.0 .

# Tag and push to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com

docker tag killuhub:0.1.0 123456789.dkr.ecr.us-east-1.amazonaws.com/killuhub:0.1.0
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/killuhub:0.1.0
```

### Part B — Deploy with Helm

```bash
# Add the pipeline YAML to a K8s ConfigMap + deploy the SparkApplication
helm upgrade --install killuhub-bronze ./helm/killuhub \
  --namespace data-platform \
  --create-namespace \
  --set platform=eks \
  --set pipeline.type=bronze \
  --set pipeline.mode=batch \
  --set "pipeline.batch.strategy=incremental" \
  --set "pipeline.connector.name=postgres" \
  --set "pipeline.bronze.table=prod.bronze.orders" \
  --set "eks.image=123456789.dkr.ecr.us-east-1.amazonaws.com/killuhub:0.1.0" \
  --set "eks.irsaRoleArn=arn:aws:iam::123456789:role/killuhub-irsa" \
  --set "eks.warehouse=s3a://my-bucket/warehouse" \
  --set "eks.catalogName=prod"
```

Or use a full `values.yaml` override file (recommended for production):

```bash
# myenv-values.yaml
platform: eks
pipeline:
  type: bronze
  mode: batch
  batch:
    strategy: incremental
    watermark_column: updated_at
  connector:
    name: postgres
    config:
      host: ""    # injected from K8s Secret via envFrom
  bronze:
    table: prod.bronze.orders
    source_name: postgres.shop.orders
    partition_by: [_ingestion_date]
eks:
  image: 123456789.dkr.ecr.us-east-1.amazonaws.com/killuhub:0.1.0
  irsaRoleArn: arn:aws:iam::123456789:role/killuhub-irsa
  warehouse: s3a://my-bucket/warehouse
  catalogName: prod
  envFrom:
    - secretRef:
        name: postgres-secret   # K8s Secret with PG_HOST, PG_PASSWORD, etc.
```

```bash
helm upgrade --install killuhub-bronze ./helm/killuhub -f myenv-values.yaml -n data-platform
```

### Part C — Watch it run

```bash
# Watch the SparkApplication CRD status
kubectl get sparkapplication killuhub-bronze-killuhub-bronze-orders -n data-platform -w

# Get driver pod name and follow logs
kubectl logs -n data-platform -l spark-role=driver -f

# Check executor pods
kubectl get pods -n data-platform -l spark-role=executor

# Describe if something fails
kubectl describe sparkapplication killuhub-bronze-... -n data-platform
```

### Part D — Streaming on EKS

For streaming (Kafka → Iceberg), the SparkApplication runs as a **long-lived pod**. The Spark Operator keeps it running:

```bash
helm upgrade --install killuhub-stream ./helm/killuhub \
  --set platform=eks \
  --set pipeline.mode=streaming \
  --set "pipeline.streaming.trigger=processingTime" \
  --set "pipeline.streaming.trigger_interval=30 seconds" \
  --set "pipeline.streaming.checkpoint_location=s3a://my-bucket/checkpoints/orders" \
  --set "pipeline.connector.stream_options.kafka\\.bootstrap\\.servers=broker:9092" \
  --set "pipeline.connector.stream_options.subscribe=orders"
```

The pod runs until cancelled. Spark Operator restarts it automatically if it crashes (using the `restartPolicy: OnFailure` in the SparkApplication spec).

---

## 12. Running on AWS EMR

### Part A — Package and upload

```bash
cd /path/to/killu-hub-framework

# Create a zip of the framework for spark-submit --py-files
pip install build
python -m build --wheel
# Also create a zip for the source tree
zip -r killuhub.zip killuhub/ main.py

# Upload to S3
aws s3 cp dist/killuhub-0.1.0-py3-none-any.whl s3://my-bucket/killuhub/
aws s3 cp killuhub.zip s3://my-bucket/killuhub/
aws s3 cp main.py s3://my-bucket/killuhub/
```

### Part B — Generate config and step with Helm

```bash
# Render the pipeline YAML
helm template killuhub-bronze ./helm/killuhub \
  --set platform=emr \
  --set pipeline.type=bronze \
  --set pipeline.connector.name=postgres \
  --set "emr.warehouse=s3://my-bucket/warehouse" \
  --show-only templates/configmap.yaml \
  | yq '.data["pipeline.yaml"]' \
  | aws s3 cp - s3://my-bucket/killuhub/configs/bronze_orders.yaml

# Render the EMR step JSON
helm template killuhub-bronze ./helm/killuhub \
  --set platform=emr \
  --show-only templates/emr-step.yaml \
  > /tmp/emr-step.json
```

### Part C — Submit the step

```bash
# Add step to an existing running cluster
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXX \
  --steps file:///tmp/emr-step.json

# Or start a transient cluster that auto-terminates after the step
aws emr create-cluster \
  --cli-input-json file:///tmp/emr-cluster.json
```

### Part D — Watch the logs

```bash
# Get step ID
aws emr list-steps --cluster-id j-XXXXXXXXXXXX

# Stream step logs
aws emr ssh --cluster-id j-XXXXXXXXXXXX --key-pair-file my-key.pem
# Inside the cluster:
yarn logs -applicationId application_xxx
```

---

## 13. Full datalake walkthrough — Postgres → Bronze → Silver on Databricks

This is the complete study scenario, step by step.

### The data model

```
PostgreSQL (source)
  └── orders table
        └── order_id, customer_id, amount, status, created_at, updated_at

Iceberg Bronze  main.bronze.orders
  └── all source columns + _ingested_at, _source_name, _batch_id, _batch_mode, _ingestion_date

Iceberg Silver  main.silver.orders
  └── typed, deduped, enriched + date dimensions + _processed_at
```

### Step 1 — Create synthetic source data (no real Postgres needed)

In a Databricks notebook:

```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("order_id",    T.LongType(),   False),
    T.StructField("customer_id", T.LongType(),   False),
    T.StructField("amount",      T.DoubleType(), False),
    T.StructField("quantity",    T.IntegerType(),False),
    T.StructField("status",      T.StringType(), False),
    T.StructField("created_at",  T.StringType(), False),
    T.StructField("updated_at",  T.StringType(), False),
])

from datetime import datetime, timedelta
import random

rows = []
statuses = ["pending","paid","shipped","cancelled","refunded"]
for i in range(1, 10001):
    dt = datetime(2024, 1, 1) + timedelta(hours=i)
    rows.append((i, random.randint(1,100), round(random.uniform(10,500),2),
                 random.randint(1,5), random.choice(statuses),
                 str(dt), str(dt)))

df = spark.createDataFrame(rows, schema)
df.createOrReplaceTempView("orders_source")
print(f"Source rows: {df.count()}")
```

### Step 2 — Run Bronze (full load)

```python
import os, sys

bronze_config = """
type: bronze
mode: batch

batch:
  strategy: full

connector:
  name: postgres       # won't actually connect — we override below
  config:
    host: localhost
    database: shop
    user: admin
    password: x
    query: "SELECT * FROM orders"

engine:
  name: spark
  warehouse: /dbfs/killuhub/warehouse
  catalog_name: main
  catalog_type: unity
  unity_catalog: main

bronze:
  table: main.bronze.orders
  source_name: postgres.shop.orders
  partition_by: [_ingestion_date]

contract:
  on_violation: warn
  min_row_count: 1
  columns:
    - name: order_id
      type: long
      nullable: false
"""

os.makedirs("/dbfs/killuhub/configs", exist_ok=True)
with open("/dbfs/killuhub/configs/bronze_orders.yaml", "w") as f:
    f.write(bronze_config)
```

> **Using synthetic data instead of Postgres?** Write directly:
> ```python
> df.writeTo("main.bronze.orders").createOrReplace()
> spark.sql("ALTER TABLE main.bronze.orders ADD COLUMNS (_ingested_at TIMESTAMP, _source_name STRING, _batch_id STRING, _batch_mode STRING, _ingestion_date DATE)")
> df2 = df.withColumn("_ingested_at", F.current_timestamp()) \
>         .withColumn("_source_name", F.lit("postgres.shop.orders")) \
>         .withColumn("_batch_id", F.lit("test-batch-001")) \
>         .withColumn("_batch_mode", F.lit("full")) \
>         .withColumn("_ingestion_date", F.current_date())
> df2.writeTo("main.bronze.orders").createOrReplace()
> ```

### Step 3 — Inspect Bronze

```python
# Row count and metadata check
spark.sql("""
  SELECT
    _batch_mode,
    _source_name,
    _ingestion_date,
    count(*) as rows
  FROM main.bronze.orders
  GROUP BY 1,2,3
  ORDER BY 3
""").show()

# Raw sample
spark.sql("SELECT * FROM main.bronze.orders LIMIT 5").show(truncate=False)

# Iceberg snapshot history
spark.sql("SELECT * FROM main.bronze.orders.snapshots").show()
```

### Step 4 — Run Silver

```python
silver_config = """
type: silver
mode: batch

batch:
  strategy: full

engine:
  name: spark
  warehouse: /dbfs/killuhub/warehouse
  catalog_name: main
  catalog_type: unity
  unity_catalog: main

silver:
  bronze_table: main.bronze.orders
  silver_table: main.silver.orders
  key_columns: [order_id]
  date_columns: [created_at, updated_at]
  date_dimension_column: created_at
  type_map:
    amount: double
    quantity: int
  null_check_columns: [order_id, customer_id]
  drop_columns: [_batch_id]
  partition_by: [created_date]
  state_store: iceberg

contract:
  on_violation: fail
  columns:
    - name: order_id
      type: long
      nullable: false
    - name: amount
      type: double
      nullable: false
      min_value: 0
    - name: created_at
      type: timestamp
      nullable: false
    - name: created_year
      type: integer
      nullable: false
"""

with open("/dbfs/killuhub/configs/silver_orders.yaml", "w") as f:
    f.write(silver_config)

sys.argv = ["main", "--config", "/dbfs/killuhub/configs/silver_orders.yaml"]
from main import load_config, run
result = run(load_config("/dbfs/killuhub/configs/silver_orders.yaml"))
print(result)
```

### Step 5 — Inspect Silver and explore Iceberg features

```python
# Silver sample — notice typed timestamps and date dimensions
spark.sql("""
  SELECT order_id, amount, status, created_at,
         created_year, created_month, created_date
  FROM main.silver.orders
  LIMIT 10
""").show()

# Partition distribution (Iceberg hidden partitioning)
spark.sql("""
  SELECT created_date, count(*) as orders, sum(amount) as revenue
  FROM main.silver.orders
  GROUP BY created_date
  ORDER BY created_date
""").show(50)

# Compare Bronze vs Silver row counts (should differ — Silver deduplicates)
print("Bronze:", spark.sql("SELECT count(*) FROM main.bronze.orders").collect()[0][0])
print("Silver:", spark.sql("SELECT count(*) FROM main.silver.orders").collect()[0][0])

# Iceberg time travel — see the table at a previous snapshot
spark.sql("SELECT * FROM main.silver.orders.snapshots").show()
spark.sql("SELECT count(*) FROM main.silver.orders VERSION AS OF 1").show()

# Schema evolution — add a column to Bronze without breaking Silver
spark.sql("ALTER TABLE main.bronze.orders ADD COLUMN (channel STRING)")
spark.sql("SELECT * FROM main.bronze.orders LIMIT 3").show()
```

---

## 14. Full datalake walkthrough — Kafka streaming → Bronze on EKS

### The data flow

```
Kafka topic: orders  →  StreamingBronzePipeline  →  Iceberg prod.bronze.orders_stream
                              (microbatch every 30s)
                              (checkpoint on S3)
```

### Step 1 — Deploy with Helm

```bash
# Streaming values file
cat > streaming-values.yaml << 'EOF'
platform: eks
pipeline:
  type: bronze
  mode: streaming
  streaming:
    trigger: processingTime
    trigger_interval: "30 seconds"
    checkpoint_location: "s3a://my-bucket/checkpoints/orders"
    output_mode: append
  connector:
    name: kafka
    stream_format: kafka
    stream_options:
      kafka.bootstrap.servers: "kafka.data-platform.svc:9092"
      subscribe: "orders"
      startingOffsets: "latest"
      failOnDataLoss: "false"
  bronze:
    table: "prod.bronze.orders_stream"
    source_name: "kafka.orders"
    partition_by: ["_ingestion_date"]
  contract:
    on_violation: warn
    min_row_count: 0
    columns:
      - name: value
        nullable: false
eks:
  image: "123456789.dkr.ecr.us-east-1.amazonaws.com/killuhub:0.1.0"
  irsaRoleArn: "arn:aws:iam::123456789:role/killuhub-irsa"
  warehouse: "s3a://my-bucket/warehouse"
  catalogName: prod
  catalogType: hadoop
  driver:
    cores: 1
    memory: "2g"
    nodePool: spark-driver
  executor:
    cores: 2
    memory: "4g"
    nodePool: spark-executor
  dynamicAllocation:
    enabled: true
    initialExecutors: 1
    minExecutors: 1
    maxExecutors: 5
job:
  name: killuhub-orders-stream
EOF

helm upgrade --install killuhub-orders-stream ./helm/killuhub \
  -f streaming-values.yaml \
  --namespace data-platform
```

### Step 2 — Watch the stream

```bash
# The SparkApplication CRD stays in RUNNING state continuously
kubectl get sparkapplication killuhub-orders-stream -n data-platform -w

# Follow driver logs — shows microbatch completions
kubectl logs -n data-platform -l spark-role=driver,app=killuhub-orders-stream -f

# Produce test messages
kubectl run kafka-producer --rm -it --image=confluentinc/cp-kafka:7.5.0 -- \
  kafka-console-producer --bootstrap-server kafka:9092 --topic orders
# Type: {"order_id":1,"customer_id":100,"amount":99.9,"status":"paid"}
```

### Step 3 — Query the live Bronze table

From any Spark client (notebook, Databricks connected to the same catalog):
```python
# Latest microbatch
spark.sql("""
  SELECT _ingestion_date, count(*) as rows, max(_ingested_at) as last_ingested
  FROM prod.bronze.orders_stream
  GROUP BY _ingestion_date
  ORDER BY _ingestion_date DESC
""").show()
```

---

## 15. Troubleshooting

### Config error: `Required environment variable 'X' is not set`

You have `${X}` in your YAML with no default and the env var is not set.

Fix: either export the variable or add a default: `${X:-some-default}`.

### Spark can't find Iceberg classes on local

The JAR is downloaded via Maven on first run. This requires internet access and takes 1-2 minutes.

If you're offline or in a private network: bake the JAR into your Docker image (see `Dockerfile`) or place it in `/opt/spark/jars/` manually.

### On Databricks: `AnalysisException: Table not found`

The Unity Catalog may not be enabled, or the schema (database) doesn't exist yet.

Fix:
```python
spark.sql("CREATE DATABASE IF NOT EXISTS main.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS main.silver")
```

### On EKS: SparkApplication stuck in PENDING

Usually a node provisioning issue. Check Karpenter:
```bash
kubectl describe nodeclaim -n karpenter   # see if Karpenter is provisioning
kubectl describe pod -n data-platform <driver-pod>  # look at Events section
```

Common causes: wrong node selector label, spot capacity unavailable, image pull error.

### Incremental pipeline returns 0 rows on second run

The state file has a watermark equal to `max(watermark_column)` from the last run. If no new rows were added to the source since then, 0 rows is correct.

Check the state:
```bash
cat .killuhub_state/<source>__<target>.json
```

Reset to reprocess all data:
```bash
rm .killuhub_state/<source>__<target>.json
```

Or set `strategy: full` for one run.

### Contract violation: pipeline aborts with `on_violation: fail`

The Silver contract failed. The log shows exactly which rows and columns violated which rule.

Fix options:
1. Change `on_violation: warn` to investigate first without aborting
2. Fix the source data or adjust the contract rules
3. Check if a Bronze transformation is producing unexpected nulls or types
