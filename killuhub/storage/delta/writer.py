"""
Delta Lake storage writer for KilluHub (stub).

Requires: pip install delta-spark

This writer follows the same BaseStorageWriter interface as IcebergWriter,
making it a drop-in replacement via the registry.
"""
from typing import Any

from killuhub.core.storage_interface import BaseStorageWriter


class DeltaWriter(BaseStorageWriter):
    def __init__(self, spark: Any, **kwargs):
        self._spark = spark
        self.warehouse = kwargs.get("warehouse", "/tmp/killuhub-delta")
        self.merge_keys: list[str] = kwargs.get("merge_keys", [])

    def table_exists(self, table_name: str) -> bool:
        try:
            self._spark.sql(f"DESCRIBE TABLE delta.`{self._path(table_name)}`")
            return True
        except Exception:
            return False

    def write(self, df: Any, table_name: str, mode: str = "append") -> None:
        path = self._path(table_name)
        if mode in ("append", "overwrite"):
            df.write.format("delta").mode(mode).save(path)
        elif mode == "merge":
            self._merge(df, table_name, path)
        else:
            raise ValueError(f"Unsupported write mode: '{mode}'")

    def _merge(self, df: Any, table_name: str, path: str) -> None:
        if not self.merge_keys:
            raise ValueError("merge_keys must be set for mode='merge'.")

        try:
            from delta.tables import DeltaTable
        except ImportError as e:
            raise ImportError(
                "delta-spark is required. Install with: pip install delta-spark"
            ) from e

        from delta.tables import DeltaTable

        join_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in self.merge_keys
        )
        DeltaTable.forPath(self._spark, path).alias("target").merge(
            df.alias("source"), join_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def _path(self, table_name: str) -> str:
        return f"{self.warehouse}/{table_name.replace('.', '/')}"
