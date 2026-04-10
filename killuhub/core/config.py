from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConnectorConfig:
    """Generic key-value config bag for any connector."""
    params: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)

    def require(self, key: str) -> Any:
        if key not in self.params:
            from killuhub.core.exceptions import ConnectorConfigError
            raise ConnectorConfigError(
                f"Required config key '{key}' is missing."
            )
        return self.params[key]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConnectorConfig":
        return cls(params=data)


@dataclass
class PipelineConfig:
    """Configuration for a full ingestion pipeline run."""
    connector_name: str
    connector_config: ConnectorConfig
    engine_name: str = "spark"
    storage_writer_name: str = "iceberg"
    target_table: str = ""
    write_mode: str = "append"
    batch_size: int = 10_000
    transform_fn: Any = None
    engine_kwargs: dict[str, Any] = field(default_factory=dict)
    storage_kwargs: dict[str, Any] = field(default_factory=dict)
