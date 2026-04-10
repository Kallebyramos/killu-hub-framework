"""
Iceberg storage writer for KilluHub.

Writes Spark DataFrames to Apache Iceberg tables using the
Iceberg Spark runtime. Supports append, overwrite, and upsert (merge).

Required kwargs (passed via PipelineConfig.storage_kwargs):
    catalog_name  (default "local")
    warehouse     (default "/tmp/killuhub-warehouse")

Optional:
    partition_by  (list[str]) — partition columns for new tables
    merge_keys    (list[str]) — unique keys used for upsert mode
"""
from typing import Any

from killuhub.core.storage_interface import BaseStorageWriter
from killuhub.storage.iceberg.schema_manager import IcebergSchemaManager


class IcebergWriter(BaseStorageWriter):
    def __init__(self, spark: Any, **kwargs):
        self._spark = spark
        self.catalog_name = kwargs.get("catalog_name", "local")
        self.warehouse = kwargs.get("warehouse", "/tmp/killuhub-warehouse")
        self.partition_by: list[str] = kwargs.get("partition_by", [])
        self.merge_keys: list[str] = kwargs.get("merge_keys", [])
        self._schema_mgr = IcebergSchemaManager(spark, self.catalog_name)

    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        return self._schema_mgr.table_exists(table_name)

    def write(self, df: Any, table_name: str, mode: str = "append") -> None:
        """
        Write a Spark DataFrame to an Iceberg table.

        Modes:
          append    — insert new rows (default)
          overwrite — replace all data in the table
          merge     — upsert using merge_keys (requires Iceberg v2 + row lineage)
        """
        if mode == "append":
            self._append(df, table_name)
        elif mode == "overwrite":
            self._overwrite(df, table_name)
        elif mode == "merge":
            self._merge(df, table_name)
        else:
            raise ValueError(f"Unsupported write mode: '{mode}'")

    # ------------------------------------------------------------------
    # Internal write strategies
    # ------------------------------------------------------------------

    def _append(self, df: Any, table_name: str) -> None:
        if not self.table_exists(table_name):
            self._schema_mgr.create_table(
                table_name, df, partition_by=self.partition_by or None
            )
        else:
            df.writeTo(table_name).append()

    def _overwrite(self, df: Any, table_name: str) -> None:
        if not self.table_exists(table_name):
            self._schema_mgr.create_table(
                table_name, df, partition_by=self.partition_by or None
            )
        else:
            df.writeTo(table_name).overwritePartitions()

    def _merge(self, df: Any, table_name: str) -> None:
        if not self.merge_keys:
            raise ValueError(
                "merge_keys must be set in storage_kwargs for mode='merge'."
            )

        if not self.table_exists(table_name):
            self._schema_mgr.create_table(
                table_name, df, partition_by=self.partition_by or None,
                properties={"write.delete.mode": "merge-on-read",
                            "write.update.mode": "merge-on-read",
                            "write.merge.mode": "merge-on-read"},
            )
            return

        # Register source as a temp view for the MERGE statement
        df.createOrReplaceTempView("_killuhub_source")

        join_clause = " AND ".join(
            f"target.{k} = source.{k}" for k in self.merge_keys
        )

        # Build SET clause for matched rows (update all non-key columns)
        non_key_cols = [c for c in df.columns if c not in self.merge_keys]
        set_clause = ", ".join(f"target.{c} = source.{c}" for c in non_key_cols)

        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join(f"source.{c}" for c in df.columns)

        self._spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING _killuhub_source AS source
            ON {join_clause}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
