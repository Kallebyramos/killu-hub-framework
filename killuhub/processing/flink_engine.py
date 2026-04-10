"""
Apache Flink processing engine for KilluHub.

Wraps PyFlink's StreamExecutionEnvironment / TableEnvironment.
Flink is the natural choice for streaming pipelines (Kafka → Iceberg).

Note: PyFlink requires a local Flink installation or a Docker/K8s cluster.
      pip install apache-flink
"""
from typing import Any, Callable

from killuhub.core.engine_interface import BaseEngine


class FlinkEngine(BaseEngine):
    """
    PyFlink-backed processing engine.

    For batch-style use, we use the Table API which aligns well with
    the connector/storage abstraction. For streaming, override or extend
    this class and call env.execute() yourself.
    """

    def __init__(self):
        self._env = None
        self._t_env = None

    def init_session(self, app_name: str = "KilluHub-Flink", **kwargs) -> None:
        try:
            from pyflink.table import EnvironmentSettings, TableEnvironment
        except ImportError as e:
            raise ImportError(
                "apache-flink is required for FlinkEngine. "
                "Install it with: pip install apache-flink"
            ) from e

        from pyflink.table import EnvironmentSettings, TableEnvironment

        mode = kwargs.get("mode", "streaming")
        if mode == "batch":
            settings = EnvironmentSettings.in_batch_mode()
        else:
            settings = EnvironmentSettings.in_streaming_mode()

        self._t_env = TableEnvironment.create(settings)

        # Iceberg Flink catalog registration
        catalog_name = kwargs.get("catalog_name", "iceberg_catalog")
        warehouse = kwargs.get("warehouse", "/tmp/killuhub-flink-warehouse")
        self._t_env.execute_sql(f"""
            CREATE CATALOG {catalog_name} WITH (
                'type'='iceberg',
                'catalog-type'='hadoop',
                'warehouse'='{warehouse}'
            )
        """)
        self._t_env.use_catalog(catalog_name)

    def create_dataframe(self, records: list[dict[str, Any]]) -> Any:
        """
        Convert records to a Flink Table via a Pandas bridge.
        Requires pandas to be available in the environment.
        """
        if self._t_env is None:
            raise RuntimeError("Call init_session() before create_dataframe().")

        import pandas as pd
        df = pd.DataFrame(records)
        return self._t_env.from_pandas(df)

    def apply_transform(self, df: Any, transform_fn: Callable | None) -> Any:
        if transform_fn is None:
            return df
        return transform_fn(df)

    @property
    def table_env(self) -> Any:
        """Expose the raw TableEnvironment for advanced use cases."""
        return self._t_env

    def stop(self) -> None:
        # TableEnvironment has no explicit stop; reset references
        self._t_env = None
        self._env = None
