from typing import Type

from killuhub.core.connector_interface import BaseConnector
from killuhub.core.engine_interface import BaseEngine
from killuhub.core.storage_interface import BaseStorageWriter
from killuhub.core.exceptions import (
    ConnectorNotFoundError,
    EngineNotFoundError,
    StorageWriterNotFoundError,
)


class Registry:
    """
    Central registry for connectors, engines, and storage writers.

    Components register themselves by name; the pipeline resolves
    them at runtime. This makes KilluHub pluggable — third-party
    connectors/engines just need to call register_*().
    """

    def __init__(self):
        self._connectors: dict[str, Type[BaseConnector]] = {}
        self._engines: dict[str, Type[BaseEngine]] = {}
        self._writers: dict[str, Type[BaseStorageWriter]] = {}

    # --- connectors ---

    def register_connector(self, name: str, cls: Type[BaseConnector]) -> None:
        self._connectors[name] = cls

    def get_connector(self, name: str) -> Type[BaseConnector]:
        if name not in self._connectors:
            raise ConnectorNotFoundError(name)
        return self._connectors[name]

    # --- engines ---

    def register_engine(self, name: str, cls: Type[BaseEngine]) -> None:
        self._engines[name] = cls

    def get_engine(self, name: str) -> Type[BaseEngine]:
        if name not in self._engines:
            raise EngineNotFoundError(name)
        return self._engines[name]

    # --- storage writers ---

    def register_writer(self, name: str, cls: Type[BaseStorageWriter]) -> None:
        self._writers[name] = cls

    def get_writer(self, name: str) -> Type[BaseStorageWriter]:
        if name not in self._writers:
            raise StorageWriterNotFoundError(name)
        return self._writers[name]

    # --- introspection ---

    def list_connectors(self) -> list[str]:
        return list(self._connectors)

    def list_engines(self) -> list[str]:
        return list(self._engines)

    def list_writers(self) -> list[str]:
        return list(self._writers)


# Singleton instance used throughout the framework
default_registry = Registry()
