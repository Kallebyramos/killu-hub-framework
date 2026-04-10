"""
Example: Kafka → Spark → Iceberg

Consumes a batch of events from a Kafka topic, enriches them with a
Spark transform (adds a processing_date column), and upserts into
an Iceberg table using event_id as the merge key.

Run:
    python examples/kafka_to_iceberg.py
"""
from killuhub import Pipeline, PipelineConfig, ConnectorConfig


def transform(df):
    from pyspark.sql import functions as F
    return df.withColumn("processing_date", F.current_date())


config = PipelineConfig(
    connector_name="kafka",
    connector_config=ConnectorConfig.from_dict(
        {
            "bootstrap_servers": "localhost:9092",
            "topic": "user-events",
            "group_id": "killuhub-consumer",
            "auto_offset_reset": "earliest",
            "max_records": 50_000,
            "value_deserializer": "json",
        }
    ),
    engine_name="spark",
    storage_writer_name="iceberg",
    target_table="local.events.user_events",
    write_mode="merge",
    batch_size=10_000,
    transform_fn=transform,
    engine_kwargs={
        "catalog_name": "local",
        "warehouse": "/tmp/killuhub-warehouse",
    },
    storage_kwargs={
        "catalog_name": "local",
        "merge_keys": ["event_id"],
        "partition_by": ["processing_date"],
    },
)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    summary = Pipeline(config).run()
    print(summary)
