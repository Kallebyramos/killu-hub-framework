"""
Example: INCREMENTAL Silver run with IcebergStateStore (production mode)

Shows how to:
  - Use IcebergStateStore instead of the default JsonFileStateStore
  - Run Silver INCREMENTALLY over a Kafka → Bronze pipeline
  - Use a custom user transform alongside standard Silver transforms

The state is persisted in a dedicated Iceberg table (local.meta.killuhub_state),
so multiple processes or engines can share the same watermark.

Run:
    python examples/incremental_silver.py
"""
import logging

from killuhub import (
    BronzePipeline, BronzeConfig,
    SilverPipeline, SilverConfig,
    PipelineConfig, ConnectorConfig,
    BatchMode, BatchConfig,
)
from killuhub.layers.silver.state import IcebergStateStore
from killuhub.processing.spark_engine import SparkEngine

logging.basicConfig(level=logging.INFO)

ENGINE_KWARGS = {"warehouse": "/tmp/killuhub-warehouse", "catalog_name": "local"}

# ---------------------------------------------------------------------------
# Bronze: Kafka events → Bronze Iceberg
# ---------------------------------------------------------------------------

kafka_bronze = BronzeConfig(
    pipeline_config=PipelineConfig(
        connector_name="kafka",
        connector_config=ConnectorConfig.from_dict({
            "bootstrap_servers": "localhost:9092",
            "topic": "user-events",
            "group_id": "killuhub-bronze",
            "max_records": 100_000,
        }),
        engine_name="spark",
        engine_kwargs=ENGINE_KWARGS,
    ),
    batch_config=BatchConfig(
        mode=BatchMode.INCREMENTAL,
        watermark_column="event_time",
    ),
    bronze_table="local.bronze.user_events",
    source_name="kafka.user-events",
)

# ---------------------------------------------------------------------------
# Silver: Bronze → Silver with IcebergStateStore
# ---------------------------------------------------------------------------
# We need a SparkSession to instantiate IcebergStateStore.
# In production, this session would already be running.

def build_silver_config():
    # Start an engine just to get the SparkSession for the state store
    # In a real app this session would be shared / managed externally
    engine = SparkEngine()
    engine.init_session(app_name="killuhub-state-init", **ENGINE_KWARGS)
    spark = engine.spark

    state_store = IcebergStateStore(
        spark=spark,
        state_table="local.meta.killuhub_state",
        catalog_name="local",
    )

    return SilverConfig(
        bronze_table="local.bronze.user_events",
        silver_table="local.silver.user_events",
        batch_config=BatchConfig(
            mode=BatchMode.INCREMENTAL,
            watermark_column="_ingested_at",
            initial_watermark="2024-01-01T00:00:00",
        ),
        key_columns=["event_id"],
        date_columns=["event_time"],
        date_dimension_column="event_time",
        type_map={"user_id": "long", "duration_ms": "int"},
        null_check_columns=["event_id", "user_id"],
        partition_by=["event_date"],
        state_store=state_store,          # production state store
        engine_kwargs=ENGINE_KWARGS,
        storage_kwargs={"catalog_name": "local"},
    ), engine


if __name__ == "__main__":
    print("=== Bronze: Kafka → Iceberg ===")
    BronzePipeline(kafka_bronze).run()

    print("\n=== Silver: INCREMENTAL with IcebergStateStore ===")
    silver_config, engine = build_silver_config()
    try:
        summary = SilverPipeline(silver_config).run()
        print(summary)
    finally:
        engine.stop()
