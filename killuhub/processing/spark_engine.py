"""
Spark processing engine for KilluHub.

Wraps PySpark's SparkSession with environment-aware initialisation.
The same engine class works on every platform:

  LOCAL       — creates a local SparkSession with Iceberg catalog wired
  KUBERNETES  — Spark Operator sets the master; we wire the catalog
  DATABRICKS  — SparkSession is pre-initialised by the runtime; we reuse it
  EMR         — similar to local: we create the session, EMR sets YARN master

Environment detection is automatic via killuhub.core.environment.detect().
You can force a specific environment with KILLUHUB_ENV=databricks (etc.).
"""
import logging
from typing import Any, Callable

from killuhub.core.engine_interface import BaseEngine

logger = logging.getLogger(__name__)


class SparkEngine(BaseEngine):
    """
    PySpark-backed processing engine.

    Session initialisation is lazy (triggered by init_session()) and
    environment-aware. The raw SparkSession is exposed via .spark so
    advanced users can run arbitrary SQL without going through KilluHub.
    """

    def __init__(self):
        self._spark = None
        self._env = None

    # ------------------------------------------------------------------
    # BaseEngine interface
    # ------------------------------------------------------------------

    def init_session(self, app_name: str = "KilluHub", **kwargs) -> None:
        """
        Initialise (or reuse) a SparkSession for the current environment.

        Args:
            app_name:   Application name shown in Spark UI.
            **kwargs:   Environment-specific options (see below).

        Common kwargs:
            catalog_name   (str)  — Iceberg catalog name, default "local"
            catalog_type   (str)  — "hadoop" | "hive" | "rest" | "glue" | "unity"
            warehouse      (str)  — warehouse root path (S3, ADLS, DBFS, local)
            extra_config   (dict) — arbitrary spark.* config key/value pairs

        Databricks-only kwargs (ignored on other environments):
            unity_catalog  (str)  — Unity Catalog name to use (default "main")

        K8s / local only:
            iceberg_impl   (str)  — Maven coordinates for the Iceberg runtime JAR
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError(
                "pyspark is required for SparkEngine. "
                "Install it with: pip install pyspark"
            ) from e

        from killuhub.core.environment import RuntimeEnvironment, detect

        self._env = detect()
        logger.info("SparkEngine | environment=%s app=%s", self._env, app_name)

        if self._env == RuntimeEnvironment.DATABRICKS:
            self._init_databricks(SparkSession, app_name, **kwargs)
        elif self._env == RuntimeEnvironment.EMR:
            self._init_emr(SparkSession, app_name, **kwargs)
        else:
            # LOCAL and KUBERNETES share the same init path.
            # On K8s, Spark Operator injects SPARK_MASTER via env so
            # getOrCreate() picks it up automatically — we don't override it.
            self._init_standard(SparkSession, app_name, **kwargs)

    def create_dataframe(self, records: list[dict[str, Any]]) -> Any:
        if self._spark is None:
            raise RuntimeError("Call init_session() before create_dataframe().")
        return self._spark.createDataFrame(records)

    def apply_transform(self, df: Any, transform_fn: Callable | None) -> Any:
        if transform_fn is None:
            return df
        return transform_fn(df)

    def stop(self) -> None:
        from killuhub.core.environment import RuntimeEnvironment

        if self._spark and self._env != RuntimeEnvironment.DATABRICKS:
            # Never stop the session on Databricks — it's shared across
            # all jobs on the cluster. Stopping it would kill other jobs.
            self._spark.stop()
        self._spark = None

    @property
    def spark(self) -> Any:
        """Expose the raw SparkSession for advanced use cases."""
        return self._spark

    # ------------------------------------------------------------------
    # Environment-specific initialisation strategies
    # ------------------------------------------------------------------

    def _init_standard(self, SparkSession, app_name: str, **kwargs) -> None:
        """
        LOCAL and KUBERNETES initialisation.

        On LOCAL:      builds a fresh session with local[*] master.
        On KUBERNETES: Spark Operator sets SPARK_MASTER automatically,
                       so master() is NOT called — getOrCreate() reads it
                       from the Spark configuration injected by the operator.
        """
        from killuhub.core.environment import RuntimeEnvironment

        catalog_name = kwargs.get("catalog_name", "local")
        catalog_type = kwargs.get("catalog_type", "hadoop")
        warehouse    = kwargs.get("warehouse", "/tmp/killuhub-warehouse")
        iceberg_impl = kwargs.get(
            "iceberg_impl",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )

        builder = SparkSession.builder.appName(app_name)

        # On LOCAL only: set master explicitly so the session starts
        if self._env == RuntimeEnvironment.LOCAL:
            builder = builder.master("local[*]")

        builder = (
            builder
            # Iceberg runtime JAR — skipped if JAR is already in /opt/spark/jars
            # (baked into the Docker image). spark.jars.packages is a no-op if
            # the class is already on the classpath.
            .config("spark.jars.packages", iceberg_impl)
            # Register the catalog
            .config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
            # Enable Iceberg SQL extensions (MERGE INTO, time travel, etc.)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )

        # Pass any extra raw Spark config (e.g. S3A credentials, shuffle tuning)
        for key, value in kwargs.get("extra_config", {}).items():
            builder = builder.config(key, value)

        self._spark = builder.getOrCreate()
        logger.info("SparkEngine | session started | catalog=%s warehouse=%s",
                    catalog_name, warehouse)

    def _init_databricks(self, SparkSession, app_name: str, **kwargs) -> None:
        """
        Databricks initialisation.

        On Databricks the SparkSession is pre-configured by the runtime:
        - Cluster config sets master, executor resources, and Spark tuning
        - Unity Catalog is wired automatically when enabled on the workspace
        - Iceberg is supported natively (no extra JARs needed for Iceberg v2)

        We simply call getOrCreate() and optionally set the catalog context.
        We NEVER call .stop() on this session.
        """
        self._spark = SparkSession.getOrCreate()

        # If Unity Catalog is in use, set the active catalog so subsequent
        # Iceberg table references (e.g. "prod.bronze.orders") resolve correctly.
        unity_catalog = kwargs.get("unity_catalog") or kwargs.get("catalog_name", "")
        if unity_catalog:
            try:
                self._spark.sql(f"USE CATALOG {unity_catalog}")
                logger.info("SparkEngine | Databricks | active catalog set to '%s'",
                            unity_catalog)
            except Exception:
                # Workspace may not have Unity Catalog enabled — log and continue
                logger.warning(
                    "SparkEngine | Databricks | could not set catalog '%s'. "
                    "Unity Catalog may not be enabled on this workspace.",
                    unity_catalog,
                )

        logger.info("SparkEngine | Databricks session reused | "
                    "runtime=%s",
                    __import__("os").environ.get("DATABRICKS_RUNTIME_VERSION", "?"))

    def _init_emr(self, SparkSession, app_name: str, **kwargs) -> None:
        """
        AWS EMR initialisation.

        On EMR, YARN is the resource manager — master is yarn.
        We wire the Iceberg catalog pointing to S3 (or Glue, if configured).
        """
        catalog_name = kwargs.get("catalog_name", "local")
        catalog_type = kwargs.get("catalog_type", "glue")  # Glue is the natural EMR catalog
        warehouse    = kwargs.get("warehouse", "s3://my-bucket/warehouse")
        iceberg_impl = kwargs.get(
            "iceberg_impl",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("yarn")
            .config("spark.jars.packages", iceberg_impl)
            .config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        )

        for key, value in kwargs.get("extra_config", {}).items():
            builder = builder.config(key, value)

        self._spark = builder.getOrCreate()
        logger.info("SparkEngine | EMR session started | catalog=%s warehouse=%s",
                    catalog_name, warehouse)
