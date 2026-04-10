"""
Iceberg schema manager for KilluHub.

Handles schema inference from Spark DataFrames, schema evolution
(add/drop/rename columns), and CREATE TABLE DDL generation.
"""
from typing import Any


class IcebergSchemaManager:
    """
    Manages Iceberg table schemas via the Spark SQL catalog.

    This manager is intentionally thin — it delegates all schema work
    to Spark's Iceberg integration so we stay in sync with the catalog.
    """

    def __init__(self, spark: Any, catalog_name: str = "local"):
        self._spark = spark
        self.catalog_name = catalog_name

    # ------------------------------------------------------------------
    # Table introspection
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        try:
            self._spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def get_schema(self, table_name: str) -> Any:
        """Return the current StructType schema of an Iceberg table."""
        return self._spark.table(table_name).schema

    def get_history(self, table_name: str) -> Any:
        """Return snapshot history (time travel metadata)."""
        return self._spark.sql(f"SELECT * FROM {table_name}.history")

    def get_snapshots(self, table_name: str) -> Any:
        return self._spark.sql(f"SELECT * FROM {table_name}.snapshots")

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def create_table(
        self,
        table_name: str,
        df: Any,
        partition_by: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """
        Create an Iceberg table using the schema inferred from a DataFrame.

        Args:
            table_name:   Fully qualified name, e.g. "local.db.orders"
            df:           Spark DataFrame whose schema to use
            partition_by: Column names to partition by
            properties:   Extra Iceberg table properties
        """
        writer = df.writeTo(table_name)

        if partition_by:
            for col in partition_by:
                writer = writer.partitionedBy(col)

        props = {"format-version": "2", **(properties or {})}
        for k, v in props.items():
            writer = writer.tableProperty(k, v)

        writer.createOrReplace()

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------

    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        self._spark.sql(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )

    def drop_column(self, table_name: str, column_name: str) -> None:
        self._spark.sql(
            f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
        )

    def rename_column(self, table_name: str, old_name: str, new_name: str) -> None:
        self._spark.sql(
            f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
        )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def expire_snapshots(self, table_name: str, older_than_ms: int) -> None:
        """Remove old snapshots to reclaim storage space."""
        self._spark.sql(f"""
            CALL {self.catalog_name}.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than_ms}'
            )
        """)

    def rewrite_data_files(self, table_name: str) -> None:
        """Compact small files for better query performance."""
        self._spark.sql(f"""
            CALL {self.catalog_name}.system.rewrite_data_files('{table_name}')
        """)
