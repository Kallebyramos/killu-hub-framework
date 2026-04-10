# Helm Chart — Study Guide
## How KilluHub uses Helm to deploy to EKS, Databricks, and EMR from one values.yaml

KilluHub's Helm chart is the **deployment interface** that sits on top of the framework. You describe *what* you want to run (connector, pipeline type, batch mode, contract) and *where* you want to run it (platform), and the chart renders the correct deployment artifacts automatically.

---

## Table of contents

1. [Why Helm?](#1-why-helm)
2. [Chart layout](#2-chart-layout)
3. [The one-file interface — values.yaml](#3-the-one-file-interface--valuesyaml)
4. [How the engine block is injected](#4-how-the-engine-block-is-injected)
5. [EKS deployment flow](#5-eks-deployment-flow)
6. [Databricks deployment flow](#6-databricks-deployment-flow)
7. [EMR deployment flow](#7-emr-deployment-flow)
8. [Template-by-template reference](#8-template-by-template-reference)
9. [Deploying in practice](#9-deploying-in-practice)
10. [Extending the chart](#10-extending-the-chart)

---

## 1. Why Helm?

Without Helm, deploying KilluHub to three platforms means:
- Maintaining three different YAML files for EKS (SparkApplication CRD)
- Maintaining separate Databricks job JSON files
- Maintaining separate EMR step JSON files
- And for each, manually editing the `engine:` block in the pipeline config

With Helm, you fill in `values.yaml` **once**. The chart renders everything else.

```
values.yaml (one file, platform-agnostic)
    │
    ├── platform: eks        → SparkApplication + RBAC + ConfigMap
    ├── platform: databricks → Databricks Asset Bundle YAML + ConfigMap
    └── platform: emr        → EMR step JSON + cluster JSON + ConfigMap
```

---

## 2. Chart layout

```
helm/killuhub/
├── Chart.yaml                   # chart metadata (name, version, description)
├── values.yaml                  # universal user interface
└── templates/
    ├── _helpers.tpl             # shared helpers (labels, fullname, engineBlock)
    ├── configmap.yaml           # pipeline YAML rendered from values (all platforms)
    ├── spark-application.yaml   # SparkApplication CRD (eks only)
    ├── rbac.yaml                # ServiceAccount + Role + RoleBinding (eks only)
    ├── databricks-job.yaml      # Databricks Asset Bundle YAML (databricks only)
    ├── emr-step.yaml            # EMR step JSON + cluster JSON (emr only)
    └── NOTES.txt                # post-install instructions printed by helm
```

The conditional rendering is done with `{{- if eq .Values.platform "eks" }}` guards — Helm simply skips templates that don't apply to the target platform.

---

## 3. The one-file interface — values.yaml

`values.yaml` has four top-level sections:

| Section      | Purpose                                           |
|--------------|---------------------------------------------------|
| `platform`   | Target: `eks`, `databricks`, or `emr`             |
| `pipeline`   | Pipeline definition — same regardless of platform |
| `eks`        | EKS-specific cluster config (ignored on others)   |
| `databricks` | Databricks-specific config (ignored on others)    |
| `emr`        | EMR-specific config (ignored on others)           |
| `job`        | Metadata — name, labels, notifications            |

The `pipeline` section mirrors the YAML structure of the config files in `config/` — you're essentially filling in the same fields, but the chart injects the `engine:` block for you.

### Example: switching platform

From EKS to Databricks — change **one line**:

```yaml
# Before
platform: eks

# After
platform: databricks
```

Nothing else changes. The chart renders a Databricks bundle instead of a SparkApplication.

---

## 4. How the engine block is injected

The `engine:` block in the pipeline YAML is the only thing that differs between platforms (see [docs/environments/environments.md](../environments/environments.md)). The chart generates it via the `killuhub.engineBlock` helper in `_helpers.tpl`:

```
{{- define "killuhub.engineBlock" -}}
{{- if eq .Values.platform "eks" -}}
engine:
  name: spark
  warehouse: {{ .Values.eks.warehouse }}
  catalog_name: {{ .Values.eks.catalogName }}
  catalog_type: {{ .Values.eks.catalogType }}   # hadoop | rest
{{- else if eq .Values.platform "databricks" -}}
engine:
  name: spark
  warehouse: {{ .Values.databricks.warehouse }}
  catalog_name: {{ .Values.databricks.catalogName }}
  catalog_type: unity
  unity_catalog: {{ .Values.databricks.unityCatalog }}
{{- else if eq .Values.platform "emr" -}}
engine:
  name: spark
  warehouse: {{ .Values.emr.warehouse }}
  catalog_name: {{ .Values.emr.catalogName }}
  catalog_type: {{ .Values.emr.catalogType }}   # glue
{{- end }}
{{- end }}
```

This helper is called inside `configmap.yaml` to splice the correct engine block into the rendered pipeline YAML.

---

## 5. EKS deployment flow

```
helm install killuhub-bronze ./helm/killuhub --set platform=eks
    │
    ├── Renders ConfigMap  → pipeline YAML with hadoop engine block
    ├── Renders RBAC       → ServiceAccount (IRSA annotation) + Role + RoleBinding
    └── Renders SparkApplication CRD
            │
            ↓
    Spark Operator detects the CRD
            │
            ├── Schedules driver pod on spark-driver NodePool (on-demand)
            └── Schedules executor pods on spark-executor NodePool (spot)
                    │
                    └── Driver mounts ConfigMap → reads pipeline.yaml → main.py
```

### Key EKS values

| Value | What it controls |
|-------|-----------------|
| `eks.image` | Docker image with KilluHub + Iceberg JAR baked in |
| `eks.irsaRoleArn` | IAM role assumed by the driver pod for S3/Glue access |
| `eks.driver.nodePool` | Karpenter NodePool label for driver placement |
| `eks.executor.nodePool` | Karpenter NodePool label for executor placement |
| `eks.dynamicAllocation.enabled` | Enables Spark Dynamic Resource Allocation |
| `eks.catalogType` | `hadoop` (single-engine) or `rest` (Nessie/Polaris, multi-engine) |

### IRSA — no hardcoded credentials

The `rbac.yaml` template annotates the ServiceAccount with:
```yaml
eks.amazonaws.com/role-arn: <irsaRoleArn>
```
Kubernetes OIDC federation exchanges the pod's projected service account token for temporary AWS credentials. The driver and executors get S3 + Glue access without any keys in the pod spec.

---

## 6. Databricks deployment flow

Helm does **not** interact with Databricks directly. It renders artifacts that your CI/CD pipeline then uploads.

```
helm template killuhub-bronze ./helm/killuhub --set platform=databricks
    │
    ├── Renders configmap.yaml  → pipeline YAML with unity engine block
    └── Renders databricks-job.yaml → Databricks Asset Bundle YAML
            │
            ↓
    CI/CD script:
      1. Upload pipeline YAML to DBFS
      2. databricks bundle deploy
      3. databricks bundle run killuhub-job
```

### Why `SparkSession.getOrCreate()` and nothing else on Databricks

The Databricks cluster is pre-configured with Unity Catalog. KilluHub calls `SparkSession.getOrCreate()` and issues `USE CATALOG <name>`. No JAR configuration, no master setting — the cluster already has everything. The `catalog_type: unity` flag in the engine block tells `SparkEngine` to skip catalog wiring.

---

## 7. EMR deployment flow

Similar to Databricks — Helm renders JSON artifacts that your CI/CD pipeline feeds to the AWS CLI.

```
helm template killuhub-bronze ./helm/killuhub --set platform=emr
    │
    ├── Renders configmap.yaml → pipeline YAML with glue engine block
    └── Renders emr-step.yaml → emr-step.json + emr-cluster.json
            │
            ↓
    CI/CD script:
      1. Upload pipeline YAML to S3
      2. Upload killuhub.zip + main.py to S3
      3. aws emr add-steps --cluster-id $ID --steps file://emr-step.json
         OR
         aws emr create-cluster --cli-input-json file://emr-cluster.json
```

EMR uses YARN as the cluster manager (`master("yarn")`) and the AWS Glue Data Catalog as the Iceberg metadata backend. The `catalog_type: glue` flag in the engine block wires `org.apache.iceberg.aws.glue.GlueCatalog`.

---

## 8. Template-by-template reference

### `_helpers.tpl`

Named templates called from other templates via `{{ include "..." . }}`:

| Helper | Returns |
|--------|---------|
| `killuhub.name` | Chart name (default) or `job.name` |
| `killuhub.fullname` | `<release>-<job.name>` truncated to 63 chars |
| `killuhub.labels` | Standard K8s + Helm labels block |
| `killuhub.configmapName` | `<fullname>-config` |
| `killuhub.engineBlock` | Platform-specific `engine:` YAML block |
| `killuhub.sparkConf` | Iceberg extension + user sparkConf merged |

### `configmap.yaml`

Renders the complete pipeline YAML from `values.pipeline`. The `engine:` block is spliced in via `killuhub.engineBlock`. This ConfigMap is:
- Mounted into the EKS driver pod at `/etc/killuhub/`
- Extracted and uploaded to DBFS/S3 for Databricks/EMR

### `spark-application.yaml` (EKS only)

`SparkApplication` CRD with:
- Driver/executor resource requests from `values.eks`
- Karpenter `nodeSelector` for placement on the right NodePool
- ConfigMap mount for pipeline YAML
- Dynamic Allocation (if enabled)
- Prometheus monitoring endpoint

### `rbac.yaml` (EKS only)

- `ServiceAccount` with IRSA annotation
- `Role` granting the driver pod/executor pod management permissions
- `RoleBinding` linking the Role to the ServiceAccount

### `databricks-job.yaml` (Databricks only)

Databricks Asset Bundle format with:
- `python_wheel_task` pointing to the uploaded pipeline YAML on DBFS
- Cluster definition (spot, autoscale, Unity Catalog env vars)
- Optional schedule (cron)

### `emr-step.yaml` (EMR only)

Two JSON documents in one file:
- `emr-step.json` — `spark-submit` command with Glue catalog config
- `emr-cluster.json` — transient cluster definition for one-shot runs

---

## 9. Deploying in practice

### EKS

```bash
# Install / upgrade
helm upgrade --install killuhub-bronze ./helm/killuhub \
  --namespace data-platform \
  --set platform=eks \
  --set pipeline.bronze.table=prod.bronze.orders \
  --set eks.image=registry.example.com/killuhub:v0.1.0 \
  --set eks.irsaRoleArn=arn:aws:iam::123456789012:role/killuhub-irsa

# Watch the SparkApplication
kubectl get sparkapplication -n data-platform -w

# Dry-run (template only, no cluster interaction)
helm template killuhub-bronze ./helm/killuhub --set platform=eks
```

### Databricks

```bash
# Render artifacts
helm template killuhub-bronze ./helm/killuhub --set platform=databricks \
  > /tmp/rendered.yaml

# Extract and upload pipeline YAML
yq '.data["pipeline.yaml"]' /tmp/configmap.yaml \
  | databricks fs cp - dbfs:/killuhub/configs/killuhub-bronze.yaml

# Deploy bundle
databricks bundle deploy && databricks bundle run killuhub-job
```

### EMR

```bash
# Render artifacts
helm template killuhub-bronze ./helm/killuhub --set platform=emr

# Upload pipeline YAML to S3
yq '.data["pipeline.yaml"]' /tmp/configmap.yaml \
  | aws s3 cp - s3://my-bucket/killuhub/configs/killuhub-bronze.yaml

# Add step to running cluster
aws emr add-steps --cluster-id j-XXXXXXXXXXXX \
  --steps file://emr-step.json
```

### Override values per environment without editing values.yaml

```bash
# Production overrides
helm upgrade --install killuhub-bronze ./helm/killuhub \
  -f helm/killuhub/values.yaml \
  -f environments/prod-overrides.yaml   # just overrides, not a full copy
```

---

## 10. Extending the chart

### Adding a new platform (e.g., Azure HDInsight)

1. Add a new `azure_hdi:` section in `values.yaml` with HDI-specific fields
2. Add a new branch to `killuhub.engineBlock` in `_helpers.tpl`:
   ```
   {{- else if eq .Values.platform "azure_hdi" -}}
   engine:
     name: spark
     warehouse: {{ .Values.azure_hdi.warehouse }}
     catalog_name: {{ .Values.azure_hdi.catalogName }}
     catalog_type: hive
   ```
3. Create `templates/azure-hdi-step.yaml` with `{{- if eq .Values.platform "azure_hdi" }}`

That's three files — consistent with how KilluHub's own plugin model works (see [docs/environments/environments.md](../environments/environments.md)).

### Adding a new pipeline type (e.g., gold)

Add a new conditional block in `configmap.yaml`:
```yaml
{{- if eq .Values.pipeline.type "gold" }}
gold:
  silver_table: {{ .Values.pipeline.gold.silver_table }}
  ...
{{- end }}
```

The `SparkApplication`, `RBAC`, and Databricks/EMR templates don't need to change — they're pipeline-type agnostic.
