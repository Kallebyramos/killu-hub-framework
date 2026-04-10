# Environment Agnosticism — Study Guide
## How KilluHub runs on K8s, Databricks, EMR, and locally

One of the core goals of KilluHub is to be **environment-agnostic**: the same pipeline code, the same YAML config structure, and the same `main.py` entry point run identically on every platform. This document explains how that works and what "agnostic" means in practice.

---

## Table of contents

1. [What actually changes between environments](#1-what-actually-changes-between-environments)
2. [core/environment.py — detecting the runtime](#2-coreenvironmentpy--detecting-the-runtime)
3. [SparkEngine — the environment-aware layer](#3-sparkengine--the-environment-aware-layer)
4. [The same framework, four environments](#4-the-same-framework-four-environments)
5. [Kubernetes (Spark Operator + Karpenter)](#5-kubernetes-spark-operator--karpenter)
6. [Databricks](#6-databricks)
7. [AWS EMR](#7-aws-emr)
8. [Local development](#8-local-development)
9. [Config differences between environments](#9-config-differences-between-environments)
10. [The "plug it in" model — how to add a new environment](#10-the-plug-it-in-model--how-to-add-a-new-environment)

---

## 1. What actually changes between environments

The honest answer: almost nothing.

```
┌──────────────────────────────────────────────────────────────────┐
│                     KilluHub pipeline                            │
│                                                                  │
│  Connector → Engine → Transform → Contract → Writer              │
│   (same)     (same)    (same)      (same)     (same)             │
│                │                                                  │
│         ┌──────┴──────────────────────────────────────┐          │
│         │          SparkEngine.init_session()          │          │
│         │                                              │          │
│         │   detect_environment()                       │          │
│         │       ├── DATABRICKS → getOrCreate()         │ ← ONLY   │
│         │       ├── KUBERNETES  → build with K8s config│   THIS   │
│         │       ├── EMR         → build with YARN      │  CHANGES │
│         │       └── LOCAL       → build with local[*]  │          │
│         └──────────────────────────────────────────────┘          │
└──────────────────────────────────────────────────────────────────┘
```

That's it. The only thing that differs between environments is **how the SparkSession is obtained**. Every layer above that — connectors, transformations, contracts, Iceberg writers — is pure PySpark and runs identically everywhere.

### What doesn't change

| Component             | Environment dependency |
|-----------------------|------------------------|
| BaseConnector / ABCs  | None                   |
| PostgresConnector     | None                   |
| KafkaConnector        | None                   |
| S3Connector           | None                   |
| RestApiConnector      | None                   |
| Transformations       | None                   |
| ContractValidator     | None (pure PySpark)    |
| IcebergWriter         | None (pure Spark SQL)  |
| BronzePipeline        | None                   |
| SilverPipeline        | None                   |
| main.py               | None                   |

### What changes

| Component              | How it changes                                              |
|------------------------|-------------------------------------------------------------|
| SparkEngine.init_session | Session creation strategy per environment                |
| YAML `engine` section  | `catalog_type`, `unity_catalog` differ per platform        |
| Job submission         | kubectl (K8s) vs Databricks Jobs API vs spark-submit (EMR) |
| Secret injection       | K8s Secrets vs Databricks Secret Scope vs AWS SSM          |

---

## 2. core/environment.py — detecting the runtime

**File:** [killuhub/core/environment.py](../../killuhub/core/environment.py)

Each platform sets a unique environment variable automatically in every process:

| Platform    | Variable                      | Set by                     |
|-------------|-------------------------------|----------------------------|
| Databricks  | `DATABRICKS_RUNTIME_VERSION`  | Databricks cluster init    |
| Kubernetes  | `KUBERNETES_SERVICE_HOST`     | K8s downward API (auto)    |
| AWS EMR     | `EMR_CLUSTER_ID`              | EMR bootstrap action       |
| Local       | (none of the above)           | —                          |

```python
from killuhub.core.environment import detect, RuntimeEnvironment

env = detect()
# Returns RuntimeEnvironment.DATABRICKS on a Databricks cluster
# Returns RuntimeEnvironment.KUBERNETES inside a K8s pod
# Returns RuntimeEnvironment.LOCAL on your laptop
```

You can also force an environment for testing:

```bash
KILLUHUB_ENV=databricks python main.py --config config/bronze.yaml
```

This is useful when:
- Testing Databricks code paths locally with a mocked SparkSession
- CI/CD environments that don't set platform-specific variables
- Debugging environment detection issues

---

## 3. SparkEngine — the environment-aware layer

**File:** [killuhub/processing/spark_engine.py](../../killuhub/processing/spark_engine.py)

`SparkEngine.init_session()` calls `detect_environment()` once and delegates to one of three strategies:

### `_init_standard()` — LOCAL and KUBERNETES

Creates a new SparkSession. On LOCAL, sets `master("local[*]")`. On KUBERNETES, does **not** set master — Spark Operator injects it via the `SPARK_MASTER` environment variable, which `getOrCreate()` reads automatically.

```python
# LOCAL
SparkSession.builder.appName("KilluHub").master("local[*]").config(...)

# KUBERNETES (Spark Operator sets SPARK_MASTER=k8s://https://...)
SparkSession.builder.appName("KilluHub").config(...)  # no .master() call
```

Both wire the Iceberg catalog (catalog name, type, warehouse path) and load the Iceberg Spark extensions.

### `_init_databricks()` — DATABRICKS

Calls `SparkSession.getOrCreate()` and stops there. The cluster is already configured.

```python
self._spark = SparkSession.getOrCreate()
# Optionally set the active catalog
self._spark.sql(f"USE CATALOG {unity_catalog}")
```

Key rules on Databricks:
- **Never call `spark.stop()`** — the session is shared across all jobs on the cluster. `SparkEngine.stop()` checks the environment and skips the stop call on Databricks.
- **Never configure `spark.jars.packages`** — Databricks handles JAR distribution at the cluster level.
- **Never set `spark.master`** — Databricks Photon engine manages this.

### `_init_emr()` — EMR

Creates a session with `master("yarn")` and wires the Glue Data Catalog as the Iceberg catalog backend:

```python
SparkSession.builder
    .master("yarn")
    .config(f"spark.sql.catalog.{name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
```

EMR + Glue is the natural AWS pairing: Glue stores the Iceberg metadata; S3 holds the data files.

---

## 4. The same framework, four environments

```
config/bronze_postgres.yaml    (same YAML structure across all envs)
         │
         ▼
    main.py --config bronze_postgres.yaml
         │
         ▼
    BronzePipeline.run()
         │
         ▼
    SparkEngine.init_session()
         │
    detect_environment()
         │
    ┌────┴─────────────────────────────────────────────────────┐
    │           │                │                  │           │
    ▼           ▼                ▼                  ▼           │
 LOCAL      KUBERNETES      DATABRICKS             EMR          │
 local[*]   K8s master      getOrCreate()      yarn master     │
 Hadoop     Hadoop/REST     Unity Catalog       Glue Catalog    │
 catalog    catalog         (already set)       catalog         │
    │           │                │                  │           │
    └───────────┴────────────────┴──────────────────┘           │
                                 │                              │
                                 ▼                              │
                     SparkSession (platform-native)             │
                                 │                              │
                                 ▼                              │
                     connector → transform → contract → Iceberg ─┘
                                 (identical on all platforms)
```

---

## 5. Kubernetes (Spark Operator + Karpenter)

See [docs/operations/study.md](../operations/study.md) for the full K8s guide.

**How the environment is detected:** `KUBERNETES_SERVICE_HOST` is injected by K8s into every pod automatically. No configuration needed.

**SparkSession:** `_init_standard()` without `master()`. Spark Operator sets `spark.master=k8s://...` via environment.

**Config delivery:** ConfigMap mounted into the driver pod at `/etc/killuhub/pipeline.yaml`. Secrets via `envFrom.secretRef`.

**Catalog:** Hadoop filesystem catalog pointing to S3, or REST catalog (Nessie/Polaris) for multi-engine access.

```yaml
# K8s engine config
engine:
  name: spark
  warehouse: s3a://my-bucket/warehouse
  catalog_name: prod
  catalog_type: hadoop    # or "rest" for Nessie
```

---

## 6. Databricks

**How the environment is detected:** `DATABRICKS_RUNTIME_VERSION` is always set by the Databricks cluster. Always. No configuration needed.

**SparkSession:** `SparkSession.getOrCreate()` — reuses the cluster's session. The session is pre-configured with the cluster's Spark config, so no catalog wiring is done in code.

**Config delivery:** Store the YAML in DBFS (`/dbfs/killuhub/configs/`) or a Databricks Repo, then reference it in the Job definition. Secrets come from Databricks Secret Scopes, exposed as environment variables in the cluster config.

**Catalog:** Unity Catalog (Databricks-native Iceberg support). The `USE CATALOG <name>` call sets the active catalog context so `bronze.orders` resolves to `<catalog>.bronze.orders`.

```yaml
# Databricks engine config
engine:
  name: spark
  warehouse: s3://my-data-lake/warehouse
  catalog_name: main          # Unity Catalog name
  catalog_type: unity         # tells SparkEngine to skip catalog wiring
  unity_catalog: main         # sets USE CATALOG
```

### Submitting on Databricks

```bash
# Via Databricks CLI
databricks jobs create --json @databricks/job-bronze-orders.json
databricks jobs run-now --job-id <id>

# Via Terraform (recommended for production)
resource "databricks_job" "bronze_orders" {
  name = "killuhub-bronze-orders"
  ...
}
```

### Databricks-specific behaviours

| Behaviour           | Why                                                        |
|---------------------|------------------------------------------------------------|
| No `spark.stop()`   | Session is shared on the cluster — stopping it kills other jobs |
| No JAR packages     | Cluster Libraries handle JAR distribution at cluster level |
| `USE CATALOG`       | Unity Catalog requires setting the catalog context before queries |
| SPOT_WITH_FALLBACK  | Databricks handles spot interruptions at the cluster level |

### Unity Catalog vs Hadoop catalog

| Feature              | Unity Catalog (Databricks) | Hadoop (K8s/local)        |
|----------------------|---------------------------|---------------------------|
| Multi-engine         | Yes (Spark, SQL Warehouse) | Spark only                |
| Governance / lineage | Yes (Unity Catalog)        | Manual                    |
| Fine-grained access  | Yes (column-level)         | No                        |
| External engines     | Limited                    | Any Iceberg client        |
| Catalog location     | Databricks-managed         | File system (S3/HDFS)     |

---

## 7. AWS EMR

**How the environment is detected:** Set `EMR_CLUSTER_ID` in your EMR bootstrap action, or use `KILLUHUB_ENV=emr`.

**SparkSession:** `_init_emr()` creates a session with `master("yarn")` and wires the Glue Data Catalog as the Iceberg catalog backend.

**Config delivery:** Store the YAML on S3. The EMR step references it:

```bash
# EMR Step
spark-submit \
  --py-files s3://my-bucket/killuhub.zip \
  s3://my-bucket/main.py \
  --config s3://my-bucket/configs/bronze_postgres.yaml
```

Or using EMR on EKS (same as K8s path, just different cluster).

```yaml
# EMR engine config
engine:
  name: spark
  warehouse: s3://my-data-lake/warehouse
  catalog_name: glue_catalog
  catalog_type: glue
  extra_config:
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
```

---

## 8. Local development

**How the environment is detected:** None of the platform variables are set → `detect()` returns `LOCAL`.

**SparkSession:** `_init_standard()` with `master("local[*]")` — runs everything in one process using all CPU cores.

**Config delivery:** Plain YAML file on disk.

**Catalog:** Hadoop filesystem catalog on a local temp directory.

```bash
# Local run with defaults
python main.py --config config/bronze_postgres.yaml

# Verify config parsing without running
python main.py --config config/bronze_postgres.yaml --dry-run

# Force a specific environment (useful for testing env detection)
KILLUHUB_ENV=databricks python main.py --dry-run
```

```yaml
# Local engine config — minimal
engine:
  name: spark
  warehouse: /tmp/killuhub-warehouse
  catalog_name: local
  catalog_type: hadoop
```

---

## 9. Config differences between environments

The YAML structure is the same everywhere. Only the `engine` section varies:

| Field          | Local                   | K8s                      | Databricks              | EMR                     |
|----------------|-------------------------|--------------------------|-------------------------|-------------------------|
| `catalog_type` | `hadoop`                | `hadoop` / `rest`        | `unity`                 | `glue`                  |
| `warehouse`    | `/tmp/killuhub-*`       | `s3a://bucket/warehouse` | `s3://bucket/warehouse` | `s3://bucket/warehouse` |
| `catalog_name` | `local`                 | `prod`                   | `main`                  | `glue_catalog`          |
| `unity_catalog`| —                       | —                        | `main`                  | —                       |

Everything else — `connector`, `batch`, `bronze/silver`, `contract` — is identical.

---

## 10. The "plug it in" model — how to add a new environment

Adding a new execution environment requires exactly three steps:

### Step 1 — Add to `RuntimeEnvironment`

```python
# core/environment.py
class RuntimeEnvironment(str, Enum):
    LOCAL      = "local"
    KUBERNETES = "kubernetes"
    DATABRICKS = "databricks"
    EMR        = "emr"
    AZURE_HDI  = "azure_hdi"   # ← new
```

### Step 2 — Add detection logic

```python
def detect() -> RuntimeEnvironment:
    ...
    if os.environ.get("HADOOP_HIVE_HOME"):  # HDInsight sets this
        return RuntimeEnvironment.AZURE_HDI
    ...
```

### Step 3 — Add init strategy in `SparkEngine`

```python
def init_session(self, app_name, **kwargs):
    ...
    elif self._env == RuntimeEnvironment.AZURE_HDI:
        self._init_azure_hdi(SparkSession, app_name, **kwargs)
    ...

def _init_azure_hdi(self, SparkSession, app_name, **kwargs):
    # Azure HDInsight uses YARN + Azure Data Lake Storage
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("yarn")
        .config("spark.sql.catalog.hdi",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hdi.type", "hive")
        .config("spark.sql.catalog.hdi.warehouse",
                kwargs.get("warehouse", "abfss://container@account.dfs.core.windows.net/"))
    )
    self._spark = builder.getOrCreate()
```

That's it. Three changes, all in the framework layer. Every pipeline, connector, transformation, and contract continues to work without any modification.

---

## Summary

The agnosticism is real and complete:

```
Same pipeline YAML
    ↓
Same main.py
    ↓
Same BronzePipeline / SilverPipeline
    ↓
Same connectors, transforms, contracts, Iceberg writers
    ↓
SparkEngine.init_session() ← only this knows about the environment
    ↓
Native SparkSession (however the platform provides it)
    ↓
Same Spark DataFrame API everywhere
    ↓
Same Iceberg write operations everywhere
```

You can develop locally with a 1,000 row sample, run CI/CD on K8s, and run production on Databricks — using the exact same YAML config and the exact same Python code.
