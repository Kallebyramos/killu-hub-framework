from abc import ABC, abstractmethod
from typing import Any, Iterator

from killuhub.core.config import ConnectorConfig


class BaseConnector(ABC):
    """
    Abstract base class for all KilluHub connectors.

    Each connector is responsible for:
    - Establishing a connection to the source
    - Extracting data as an iterator of records
    - Properly closing/releasing resources
    """

    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""

    @abstractmethod
    def extract(self) -> Iterator[dict[str, Any]]:
        """
        Yield records from the source one at a time.
        Each record is a plain dict representing a row/event/document.
        """

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by the connector."""

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    @property
    def name(self) -> str:
        return self.__class__.__name__
