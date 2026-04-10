from abc import ABC, abstractmethod
from typing import Any


class BaseEngine(ABC):
    """
    Abstract base class for processing engines (Spark, Flink, etc.).

    The engine receives raw records from a connector and applies
    user-defined transformations before handing off to storage.
    """

    @abstractmethod
    def init_session(self, app_name: str, **kwargs) -> None:
        """Initialize the engine session/context."""

    @abstractmethod
    def create_dataframe(self, records: list[dict[str, Any]]) -> Any:
        """Convert raw records into the engine's native DataFrame type."""

    @abstractmethod
    def apply_transform(self, df: Any, transform_fn) -> Any:
        """Apply a user-supplied transformation function to the DataFrame."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the engine session and release resources."""

    def __enter__(self) -> "BaseEngine":
        return self

    def __exit__(self, *_) -> None:
        self.stop()

    @property
    def name(self) -> str:
        return self.__class__.__name__
