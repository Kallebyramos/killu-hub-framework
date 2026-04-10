"""
Apache Hudi storage writer for KilluHub (stub).

Requires: spark-submit with hudi-spark bundle JAR, or
          add hudi-spark to spark.jars.packages.

This writer follows the same BaseStorageWriter interface as IcebergWriter.
"""
from typing import Any

from killuhub.core.storage_interface import BaseStorageWriter


class HudiWriter(BaseStorageWriter):
    def __init__(self, spark: Any, **kwargs):
        self._spark = spark
        self.warehouse = kwargs.get("warehouse", "/tmp/killuhub-hudi")
        self.table_type = kwargs.get("table_type", "COPY_ON_WRITE")  # or MERGE_ON_READ
        self.record_key_field: str = kwargs.get("record_key_field", "id")
        self.precombine_field: str = kwargs.get("precombine_field", "updated_at")
        self.partition_path_field: str = kwargs.get("partition_path_field", "")

    def table_exists(self, table_name: str) -> bool:
        import os
        path = self._path(table_name)
        return os.path.exists(path)

    def write(self, df: Any, table_name: str, mode: str = "append") -> None:
        hudi_mode = "upsert" if mode == "merge" else mode
        options = {
            "hoodie.table.name": table_name.split(".")[-1],
            "hoodie.datasource.write.recordkey.field": self.record_key_field,
            "hoodie.datasource.write.precombine.field": self.precombine_field,
            "hoodie.datasource.write.partitionpath.field": self.partition_path_field,
            "hoodie.datasource.write.table.type": self.table_type,
            "hoodie.datasource.write.operation": hudi_mode,
            "hoodie.datasource.hive_sync.enable": "false",
        }
        (
            df.write.format("hudi")
            .options(**options)
            .mode("append" if mode == "append" else "overwrite")
            .save(self._path(table_name))
        )

    def _path(self, table_name: str) -> str:
        return f"{self.warehouse}/{table_name.replace('.', '/')}"
