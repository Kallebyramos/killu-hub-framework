{{/*
KilluHub Helm helpers
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "killuhub.name" -}}
{{- default .Chart.Name .Values.job.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full name: release-name + job name (truncated to 63 chars, K8s label limit).
*/}}
{{- define "killuhub.fullname" -}}
{{- printf "%s-%s" .Release.Name .Values.job.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "killuhub.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "killuhub.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
platform: {{ .Values.platform }}
{{- range $k, $v := .Values.job.labels }}
{{ $k }}: {{ $v | quote }}
{{- end }}
{{- end }}

{{/*
ConfigMap name — shared between the SparkApplication and the configmap template.
*/}}
{{- define "killuhub.configmapName" -}}
{{- printf "%s-config" (include "killuhub.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Render the `engine:` block for the pipeline YAML based on the target platform.
Indented 2 spaces so it fits inside the ConfigMap data string.
*/}}
{{- define "killuhub.engineBlock" -}}
{{- if eq .Values.platform "eks" -}}
engine:
  name: spark
  warehouse: {{ .Values.eks.warehouse | quote }}
  catalog_name: {{ .Values.eks.catalogName | quote }}
  catalog_type: {{ .Values.eks.catalogType | quote }}
  {{- if eq .Values.eks.catalogType "rest" }}
  rest_catalog_uri: {{ .Values.eks.restCatalogUri | quote }}
  {{- end }}
{{- else if eq .Values.platform "databricks" -}}
engine:
  name: spark
  warehouse: {{ .Values.databricks.warehouse | quote }}
  catalog_name: {{ .Values.databricks.catalogName | quote }}
  catalog_type: unity
  unity_catalog: {{ .Values.databricks.unityCatalog | quote }}
{{- else if eq .Values.platform "emr" -}}
engine:
  name: spark
  warehouse: {{ .Values.emr.warehouse | quote }}
  catalog_name: {{ .Values.emr.catalogName | quote }}
  catalog_type: {{ .Values.emr.catalogType | quote }}
{{- end }}
{{- end }}

{{/*
Merge default sparkConf with Iceberg extension (always required).
*/}}
{{- define "killuhub.sparkConf" -}}
spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
{{- range $k, $v := . }}
{{ $k }}: {{ $v | quote }}
{{- end }}
{{- end }}
