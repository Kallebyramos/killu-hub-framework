from abc import ABC, abstractmethod
from typing import Any


class BaseStorageWriter(ABC):
    """
    Abstract base class for storage writers.

    Implementations write engine DataFrames to a table format
    (Iceberg, Delta, Hudi) on a given storage backend (S3, GCS, ADLS, local).
    """

    @abstractmethod
    def write(self, df: Any, table_name: str, mode: str = "append") -> None:
        """
        Write a DataFrame to the target table.

        Args:
            df: Engine-native DataFrame (Spark, Flink, pandas, etc.)
            table_name: Fully qualified table name (e.g. "catalog.db.table")
            mode: Write mode — "append" | "overwrite" | "merge"
        """

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Return True if the target table already exists."""

    @property
    def name(self) -> str:
        return self.__class__.__name__
