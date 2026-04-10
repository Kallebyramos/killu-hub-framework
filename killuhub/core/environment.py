"""
Runtime environment detection for KilluHub.

The same pipeline code runs on multiple platforms. The only thing that
changes between environments is how the SparkSession is obtained and how
the Iceberg catalog is configured. Everything else — connectors,
transformations, contracts, storage writers — is identical.

Detection is based on well-known environment variables that each platform
sets automatically in every process:

  DATABRICKS_RUNTIME_VERSION   → set by Databricks runtime on every cluster
  KUBERNETES_SERVICE_HOST      → set by K8s in every pod
  EMR_CLUSTER_ID               → set by AWS EMR bootstrap
  (none of the above)          → local / dev machine

You can also force a specific environment by setting:
  KILLUHUB_ENV=databricks | kubernetes | emr | local
"""
from __future__ import annotations

import os
from enum import Enum


class RuntimeEnvironment(str, Enum):
    """
    Known runtime environments KilluHub can run in.

    Using `str` as a mixin so values work directly in log messages
    and YAML comparisons without .value.
    """
    LOCAL       = "local"
    KUBERNETES  = "kubernetes"
    DATABRICKS  = "databricks"
    EMR         = "emr"


def detect() -> RuntimeEnvironment:
    """
    Detect the current runtime environment.

    Priority order:
    1. KILLUHUB_ENV — explicit override (highest priority)
    2. DATABRICKS_RUNTIME_VERSION — Databricks auto-sets this
    3. KUBERNETES_SERVICE_HOST — K8s injects this into every pod
    4. EMR_CLUSTER_ID — set by EMR bootstrap action
    5. LOCAL — fallback
    """
    forced = os.environ.get("KILLUHUB_ENV", "").lower()
    if forced:
        try:
            return RuntimeEnvironment(forced)
        except ValueError:
            valid = [e.value for e in RuntimeEnvironment]
            raise ValueError(
                f"KILLUHUB_ENV='{forced}' is not valid. "
                f"Must be one of: {valid}"
            )

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return RuntimeEnvironment.DATABRICKS

    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return RuntimeEnvironment.KUBERNETES

    if os.environ.get("EMR_CLUSTER_ID"):
        return RuntimeEnvironment.EMR

    return RuntimeEnvironment.LOCAL


def is_databricks() -> bool:
    return detect() == RuntimeEnvironment.DATABRICKS


def is_kubernetes() -> bool:
    return detect() == RuntimeEnvironment.KUBERNETES


def is_local() -> bool:
    return detect() == RuntimeEnvironment.LOCAL
