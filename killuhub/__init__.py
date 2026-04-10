"""
KilluHub — Pluggable data ingestion framework.

Sources → Connectors → Engine (Spark/Flink) → Bronze → Silver → Iceberg

Quick start (raw pipeline):
    from killuhub import Pipeline, PipelineConfig, ConnectorConfig

    Pipeline(PipelineConfig(
        connector_name="postgres",
        connector_config=ConnectorConfig.from_dict({...}),
        target_table="local.db.orders",
    )).run()

Medallion (Bronze → Silver):
    from killuhub import BronzePipeline, BronzeConfig, SilverPipeline, SilverConfig
    from killuhub.core.batch import BatchMode, BatchConfig

    BronzePipeline(BronzeConfig(
        pipeline_config=PipelineConfig(...),
        batch_config=BatchConfig(mode=BatchMode.FULL),
        bronze_table="local.bronze.orders",
        source_name="postgres.shop.orders",
    )).run()

    SilverPipeline(SilverConfig(
        bronze_table="local.bronze.orders",
        silver_table="local.silver.orders",
        batch_config=BatchConfig(mode=BatchMode.INCREMENTAL),
        key_columns=["order_id"],
        date_columns=["created_at"],
    )).run()
"""
# Import subpackages so auto-registration runs
import killuhub.connectors  # noqa: F401
import killuhub.processing  # noqa: F401

from killuhub.core import (
    BaseConnector,
    BaseEngine,
    BaseStorageWriter,
    ConnectorConfig,
    PipelineConfig,
    BatchMode,
    BatchConfig,
    IncrementalState,
    default_registry,
)
from killuhub.ingestion import Pipeline, PipelineScheduler
from killuhub.layers import BronzePipeline, BronzeConfig, SilverPipeline, SilverConfig

__version__ = "0.1.0"

__all__ = [
    # Raw pipeline
    "Pipeline",
    "PipelineScheduler",
    "PipelineConfig",
    "ConnectorConfig",
    # Medallion layers
    "BronzePipeline",
    "BronzeConfig",
    "SilverPipeline",
    "SilverConfig",
    # Batch primitives
    "BatchMode",
    "BatchConfig",
    "IncrementalState",
    # Extension points
    "BaseConnector",
    "BaseEngine",
    "BaseStorageWriter",
    "default_registry",
]
