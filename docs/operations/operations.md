# Operations — Study Guide
## main.py, Data Contracts, K8s, Spark Operator, Karpenter

This guide covers how KilluHub runs in production: the YAML-driven entry point, data quality contracts, and the Kubernetes architecture (Spark Operator + Karpenter) you already have context on from the previous project.

---

## Table of contents

1. [main.py — the entry point](#1-mainpy--the-entry-point)
2. [YAML config structure](#2-yaml-config-structure)
3. [Environment variable substitution](#3-environment-variable-substitution)
4. [Data contracts](#4-data-contracts)
5. [Contract validator internals](#5-contract-validator-internals)
6. [Kubernetes architecture overview](#6-kubernetes-architecture-overview)
7. [Spark Operator — how it works](#7-spark-operator--how-it-works)
8. [SparkApplication CRD — field by field](#8-sparkapplication-crd--field-by-field)
9. [Karpenter — node provisioning](#9-karpenter--node-provisioning)
10. [Driver vs executor node pools](#10-driver-vs-executor-node-pools)
11. [IRSA — S3 access without hardcoded keys](#11-irsa--s3-access-without-hardcoded-keys)
12. [Dockerfile — building the image](#12-dockerfile--building-the-image)
13. [Full deployment walkthrough](#13-full-deployment-walkthrough)

---

## 1. main.py — the entry point

**File:** [main.py](../../main.py)

`main.py` is the single executable entry point for any KilluHub pipeline. Instead of maintaining separate Python scripts per pipeline, all pipelines are driven by YAML config files.

```
main.py
  ├── load_config(path)     — parse YAML + resolve env vars
  ├── run(cfg)              — dispatch to correct pipeline type
  │       ├── "bronze" → _build_bronze(cfg) → BronzePipeline.run()
  │       ├── "silver" → _build_silver(cfg) → SilverPipeline.run()
  │       └── "raw"    → _build_raw(cfg)    → Pipeline.run()
  └── main()                — CLI argparse, exit codes
```

### Usage

```bash
# Local development
python main.py --config config/bronze_postgres.yaml

# Dry run — parse config without running
python main.py --config config/bronze_postgres.yaml --dry-run

# Via environment variable (K8s-friendly)
KILLUHUB_CONFIG=/etc/killuhub/pipeline.yaml python main.py
```

### Exit codes

| Code | Meaning                                      |
|------|----------------------------------------------|
| `0`  | Pipeline completed successfully               |
| `1`  | Config error, pipeline error, or any exception |

Spark Operator watches the exit code to determine job success/failure and whether to retry.

---

## 2. YAML config structure

Every YAML config has a `type` field that determines which pipeline runs:

```yaml
type: bronze   # bronze | silver | raw
```

### Bronze config

```yaml
type: bronze

batch:
  mode: full           # full | incremental
  watermark_column: updated_at

connector:
  name: postgres       # matches registry name
  config:
    host: ${PG_HOST}
    database: ${PG_DATABASE}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    query: "SELECT * FROM orders"

engine:
  name: spark
  warehouse: ${WAREHOUSE_PATH}
  catalog_name: ${CATALOG_NAME}

bronze:
  table: ${CATALOG_NAME}.bronze.orders
  source_name: postgres.shop.orders

contract:
  on_violation: warn
  columns:
    - name: order_id
      type: long
      nullable: false
```

### Silver config

```yaml
type: silver

batch:
  mode: incremental
  watermark_column: _ingested_at

engine:
  name: spark
  warehouse: ${WAREHOUSE_PATH}
  catalog_name: ${CATALOG_NAME}

silver:
  bronze_table: ${CATALOG_NAME}.bronze.orders
  silver_table: ${CATALOG_NAME}.silver.orders
  key_columns: [order_id]
  date_columns: [created_at]
  date_dimension_column: created_at
  type_map:
    amount: double
  partition_by: [created_date]
  state_store: iceberg    # "json" or "iceberg"

contract:
  on_violation: fail
  columns:
    - name: order_id
      nullable: false
```

---

## 3. Environment variable substitution

YAML values can reference environment variables using two patterns:

```yaml
# Required variable — EnvironmentError if not set
password: ${PG_PASSWORD}

# Variable with default
catalog_name: ${CATALOG_NAME:-local}
warehouse: ${WAREHOUSE_PATH:-/tmp/killuhub-warehouse}
```

This makes the same YAML file work in:
- Local dev (set defaults with `:-`)
- CI/CD pipelines (set via environment)
- K8s (set via `envFrom` + Secrets, or `env` in SparkApplication CRD)

```python
# The substitution happens before YAML parsing
_ENV_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-(.*?))?\}")
```

**Why not use YAML anchors or `---` includes?**

YAML anchors only work within a single file. Environment variables work across any infrastructure — K8s secrets, CI/CD variables, `.env` files for local dev. Simpler and more portable.

---

## 4. Data contracts

A **data contract** is a formal agreement about data quality at a pipeline boundary. In KilluHub, contracts are defined in the YAML config and validated at two points:

- **Bronze**: after metadata columns are added, before writing to Iceberg. Catches source quality issues early.
- **Silver**: after all transforms, before writing to Iceberg. Guarantees output quality.

```yaml
contract:
  on_violation: fail   # fail = abort | warn = log and continue
  min_row_count: 1
  columns:
    - name: order_id
      type: long
      nullable: false

    - name: amount
      type: double
      nullable: false
      min_value: 0        # no negative amounts

    - name: status
      type: string
      allowed_values: [pending, paid, shipped, cancelled, refunded]

    - name: created_at
      type: timestamp
      max_null_pct: 0.01  # at most 1% nulls allowed
```

### Why contracts matter in production

Without contracts, bad data silently propagates:
1. Source sends `amount = -99.99` → Bronze accepts it
2. Silver doesn't catch it → Silver table has negative amounts
3. Finance dashboard shows negative revenue → business incident

With contracts:
1. Bronze contract checks `min_value: 0` → logs warning (or aborts)
2. Silver contract checks again → alerts are actionable and specific

Contracts are the equivalent of **tests at the data boundary**. They make pipelines observable.

### ContractReport in the pipeline summary

The pipeline summary dict returned by `SilverPipeline.run()` includes the contract report:

```python
{
    "run_id": "abc-123",
    "mode": "incremental",
    "records_written": 15000,
    "contract": {
        "table_name": "local.silver.orders",
        "passed": True,
        "row_count": 15000,
        "errors": 0,
        "warnings": 2,
        "violations": [
            {"column": "status", "check": "allowed_values",
             "message": "3 row(s) have values outside allowed set", "severity": "warning"}
        ]
    }
}
```

This can be forwarded to Datadog, Slack, or a monitoring Iceberg table for trend analysis.

---

## 5. Contract validator internals

**File:** [killuhub/core/contract.py](../../killuhub/core/contract.py)

The validator is designed to minimise the number of Spark actions (each action triggers a full DataFrame scan).

### Batching null checks

Instead of one action per column, all null counts are computed in one aggregation:

```python
null_agg_exprs = [
    F.sum(F.col(spec.name).isNull().cast("int")).alias(f"_null_{spec.name}")
    for spec in self.spec.columns
    if spec.name in actual_columns
]
row = df.agg(*null_agg_exprs).collect()[0]  # ONE Spark action for all columns
```

### Value constraint checks

Range and allowed-value checks require their own Spark action per column (because the aggregation expressions differ). For performance, these are only triggered when constraints are specified:

```python
# Only runs a Spark action if min_value, max_value, or allowed_values is set
if col_spec.min_value is not None or col_spec.allowed_values is not None:
    row = df.agg(*agg_exprs).collect()[0]
```

### Check count per validation run

For a DataFrame with 5 columns, all with nullable + range constraints:
- 1 action for `df.count()`
- 1 action for all null counts (batched)
- Up to 5 actions for value constraints (one per constrained column)
- Total: ~7 Spark actions for a full contract check

This is acceptable — a Spark action on a 10M row Iceberg table with predicate pushdown typically completes in seconds.

---

## 6. Kubernetes architecture overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EKS Cluster                                   │
│                                                                      │
│  ┌──────────────────┐     ┌──────────────────────────────────────┐  │
│  │  Control Plane   │     │  Data Plane (worker nodes)           │  │
│  │                  │     │                                      │  │
│  │  Spark Operator  │────▶│  spark-driver pool (on-demand)       │  │
│  │  (controller)    │     │    ┌──────────────────────┐          │  │
│  │                  │     │    │  Driver Pod           │          │  │
│  │  Karpenter       │     │    │  - runs main.py       │          │  │
│  │  (node autoscaler│     │    │  - mounts ConfigMap   │          │  │
│  │   )              │     │    │  - reads K8s Secret   │          │  │
│  └──────────────────┘     │    └──────────────────────┘          │  │
│                           │                                      │  │
│  ┌──────────────────┐     │  spark-executor pool (spot)          │  │
│  │  Triggering      │     │    ┌────────┐ ┌────────┐ ┌────────┐  │  │
│  │  (Argo Workflow  │     │    │Executor│ │Executor│ │Executor│  │  │
│  │   / CronJob /    │     │    └────────┘ └────────┘ └────────┘  │  │
│  │   Airflow DAG)   │     │                ↑                     │  │
│  └──────────────────┘     │    Karpenter provisions nodes        │  │
│                           │    on-demand when pods are pending   │  │
│                           └──────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  S3 — Iceberg warehouse (bronze/, silver/, gold/, meta/)       │  │
│  └────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 7. Spark Operator — how it works

The **Spark Operator** is a Kubernetes operator (a controller that extends K8s with custom resources). It introduces the `SparkApplication` CRD.

### Lifecycle

```
kubectl apply -f spark-application-bronze.yaml
       ↓
Spark Operator detects new SparkApplication
       ↓
Creates Driver Pod with:
  - container image specified in .spec.image
  - mainApplicationFile as the command
  - ConfigMap volumes mounted
  - Secret env vars injected
       ↓
Driver Pod starts → SparkSession.builder.getOrCreate()
  - Spark detects K8s master (set by operator via SPARK_MASTER env var)
  - Spark requests N executor pods from K8s API
       ↓
Executor Pods created (pending → running as Karpenter provisions nodes)
       ↓
main.py runs → pipeline executes → writes to S3 Iceberg
       ↓
Driver exits 0 → Spark Operator marks SparkApplication as Completed
Executor pods terminated → Karpenter consolidates (removes empty nodes)
```

### Why Spark Operator instead of spark-submit?

`spark-submit` from a local machine or a Helm chart is stateless — you submit and forget. Spark Operator brings:

- **Observability**: `kubectl get sparkapplications` shows status, events, metrics
- **Retry logic**: `restartPolicy.onFailureRetries` handles transient failures automatically
- **Prometheus integration**: driver/executor metrics scraped out of the box
- **GitOps compatible**: SparkApplication YAMLs are Kubernetes resources — version-controlled and applied via ArgoCD/Flux

---

## 8. SparkApplication CRD — field by field

**File:** [k8s/spark-application-bronze.yaml](../../k8s/spark-application-bronze.yaml)

```yaml
spec:
  type: Python                        # Python | Scala | Java | R
  pythonVersion: "3"
  mode: cluster                       # cluster = driver runs in a pod
                                      # client = driver runs locally (dev only)
  image: "registry.example.com/killuhub:latest"
  mainApplicationFile: "local:///app/main.py"  # "local://" = file inside image
  arguments: ["--config", "/etc/killuhub/pipeline.yaml"]
```

`local://` prefix tells Spark the file is already in the container image. Without it, Spark would try to download it from HDFS or HTTP.

```yaml
  sparkConf:
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.maxExecutors": "10"
    "spark.dynamicAllocation.shuffleTracking.enabled": "true"
```

**Dynamic Allocation** lets Spark scale executors up and down based on workload. Combined with Karpenter, this gives true elastic scaling:
- Job starts → Spark requests executors → Karpenter provisions nodes
- Shuffle stage completes → Spark releases executors → Karpenter terminates nodes
- Total cluster cost = exactly what was needed

`shuffleTracking.enabled` must be `true` when using Dynamic Allocation on K8s (no external shuffle service needed).

```yaml
  driver:
    configMaps:
      - name: killuhub-bronze-orders-config
        path: /etc/killuhub
    envFrom:
      - secretRef:
          name: killuhub-postgres-secret
```

The `configMaps` mount creates a file at `/etc/killuhub/pipeline.yaml` inside the driver pod — exactly where `main.py --config` expects it.

`envFrom.secretRef` injects all keys from the K8s Secret as environment variables, which the YAML `${PG_PASSWORD}` substitution then reads.

---

## 9. Karpenter — node provisioning

**File:** [k8s/karpenter-nodepool.yaml](../../k8s/karpenter-nodepool.yaml)

Karpenter replaces the Cluster Autoscaler. The key difference:

| Feature            | Cluster Autoscaler     | Karpenter                         |
|--------------------|------------------------|-----------------------------------|
| Provisioning unit  | Node group (fixed type)| Individual node (best fit)        |
| Speed              | 2-5 minutes            | < 60 seconds                      |
| Instance diversity | Fixed per group        | Picks cheapest/fastest available  |
| Consolidation      | Basic                  | Aggressive bin-packing            |
| Spot handling      | Manual fallback groups | Automatic with `fallback` policy  |

### How Karpenter watches for KilluHub pods

When Spark Operator creates executor pods, they initially have status `Pending` because no node matches their `nodeSelector`:

```yaml
nodeSelector:
  karpenter.sh/nodepool: spark-executor
```

Karpenter's controller watches for `Pending` pods with resource requests it can satisfy. It reads the NodePool spec, picks the best instance type, and launches it. The pod schedules within ~30 seconds.

### Consolidation

When executors finish and Spark Dynamic Allocation scales down, pods are terminated. Karpenter detects empty nodes and terminates them:

```yaml
disruption:
  consolidationPolicy: WhenUnderutilized
  consolidateAfter: 30s   # wait 30s after utilisation drops, then terminate
```

---

## 10. Driver vs executor node pools

We run two separate Karpenter pools because drivers and executors have different requirements:

### spark-driver pool (on-demand)

The driver is the "brain" of the Spark job. If it's interrupted:
- The entire job fails
- All executor state is lost
- You have to restart the whole pipeline

**Therefore: always on-demand**. The cost difference between spot and on-demand for one driver node is negligible compared to the cost of a failed job.

```yaml
requirements:
  - key: karpenter.sh/capacity-type
    operator: In
    values: ["on-demand"]     # no spot for drivers
```

### spark-executor pool (spot preferred)

Executors are stateless workers. If one is interrupted:
- Spark detects the lost executor (heartbeat timeout)
- Spark re-schedules the failed tasks on surviving executors
- Karpenter replaces the node

**Therefore: spot is fine**. The 60-80% cost saving is significant when you're running many executor nodes.

```yaml
requirements:
  - key: karpenter.sh/capacity-type
    operator: In
    values: ["spot", "on-demand"]  # spot preferred, on-demand fallback
  - key: node.kubernetes.io/instance-type
    operator: In
    values: [r5.2xlarge, r5.4xlarge, r6i.2xlarge, ...]  # diverse = better spot availability
```

Listing multiple instance types increases the probability of getting spot capacity — Karpenter picks whichever is cheapest/most available.

### Why r5 for executors?

`r5` = memory-optimised. Spark's performance bottleneck is almost always memory:
- Holding DataFrames in memory (to avoid serialisation)
- Caching Iceberg metadata
- Shuffle buffers

For compute-heavy transforms (ML features, complex aggregations), `c5` (compute-optimised) might be better. Profile your jobs.

---

## 11. IRSA — S3 access without hardcoded keys

**IRSA** = IAM Roles for Service Accounts.

Instead of putting AWS credentials in a K8s Secret, IRSA links a K8s ServiceAccount to an IAM role. The pod's container gets temporary credentials via the instance metadata service:

```yaml
# ServiceAccount annotated with IAM role ARN
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-driver
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/KilluHubSparkRole"
```

```yaml
# SparkApplication uses this service account
driver:
  serviceAccount: spark-driver
```

```yaml
# Spark configured to use the WebIdentity provider (reads IRSA token)
sparkConf:
  "spark.hadoop.fs.s3a.aws.credentials.provider":
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
```

The IAM role must have:
```json
{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": [
        "arn:aws:s3:::my-data-lake",
        "arn:aws:s3:::my-data-lake/*"
    ]
}
```

**No credentials in any file, no Secret rotation required.** AWS rotates the temporary credentials automatically every 15 minutes.

---

## 12. Dockerfile — building the image

**File:** [Dockerfile](../../Dockerfile)

Two-stage build:
1. **Builder** (`python:3.11-slim`): compiles C extensions (psycopg2, confluent-kafka) into `/install`
2. **Runtime** (`apache/spark-py:v3.5.1`): copies compiled packages, adds KilluHub code

```dockerfile
# Stage 1: compile
FROM python:3.11-slim AS builder
RUN pip install --prefix=/install -r requirements.txt

# Stage 2: runtime
FROM apache/spark-py:v3.5.1
COPY --from=builder /install /usr/local
COPY killuhub/ killuhub/
COPY main.py main.py
```

**Why `apache/spark-py` as base?**

Spark Operator expects a specific directory layout:
- Spark binaries at `/opt/spark/`
- Entry point at `/opt/entrypoint.sh`
- User `185` (Spark's default UID)

Building from scratch would require replicating all of this.

**Pre-downloading the Iceberg JAR:**

```dockerfile
RUN curl -fsSL \
    "https://repo1.maven.org/.../iceberg-spark-runtime-3.5_2.12-1.5.0.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime.jar
```

Without this, Spark downloads the JAR from Maven Central at startup (`spark.jars.packages`). In a private K8s cluster with no egress to the internet, this fails silently. Baking the JAR into the image makes startup deterministic and fast.

### Build and push

```bash
# Build
docker build -t registry.example.com/killuhub:1.0.0 .
docker tag registry.example.com/killuhub:1.0.0 registry.example.com/killuhub:latest

# Push
docker push registry.example.com/killuhub:1.0.0
docker push registry.example.com/killuhub:latest
```

Tag by version — never use `latest` in production SparkApplication YAMLs. Use `1.0.0` so a rollback is `kubectl set image`.

---

## 13. Full deployment walkthrough

```bash
# 1. Create namespace
kubectl create namespace data-platform

# 2. Install Spark Operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --set sparkJobNamespace=data-platform \
  --set webhook.enable=true

# 3. Install Karpenter (EKS-specific — see Karpenter docs for your version)
# kubectl apply -f k8s/karpenter-nodepool.yaml

# 4. Apply RBAC
kubectl apply -f k8s/rbac.yaml -n data-platform

# 5. Create Secret with database credentials
kubectl create secret generic killuhub-postgres-secret \
  --from-literal=PG_HOST=prod-db.internal \
  --from-literal=PG_DATABASE=shop \
  --from-literal=PG_USER=readonly \
  --from-literal=PG_PASSWORD=supersecret \
  -n data-platform

# 6. Apply ConfigMaps (pipeline YAML configs)
kubectl apply -f k8s/configmap.yaml -n data-platform

# 7. Build and push the image
docker build -t registry.example.com/killuhub:1.0.0 .
docker push registry.example.com/killuhub:1.0.0

# 8. Submit Bronze job
kubectl apply -f k8s/spark-application-bronze.yaml -n data-platform

# 9. Watch it run
kubectl get sparkapplications -n data-platform
kubectl logs -l sparkoperator.k8s.io/app-name=killuhub-bronze-orders \
             -n data-platform -f

# 10. Submit Silver job (after Bronze completes)
kubectl apply -f k8s/spark-application-silver.yaml -n data-platform
```

### Automating the sequence (Argo Workflows)

Bronze must complete before Silver runs. This is typically handled by a DAG orchestrator:

```yaml
# Argo Workflow (simplified)
apiVersion: argoproj.io/v1alpha1
kind: Workflow
spec:
  templates:
    - name: bronze-orders
      resource:
        action: apply
        manifest: |
          # (spark-application-bronze.yaml content)

    - name: silver-orders
      dependencies: [bronze-orders]  # runs after bronze succeeds
      resource:
        action: apply
        manifest: |
          # (spark-application-silver.yaml content)
```

Airflow with the `SparkKubernetesOperator` is an equally valid option.
