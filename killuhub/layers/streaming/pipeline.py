"""
Streaming Bronze layer pipeline for KilluHub.

Reads from a streaming source (Kafka, file, socket) using Spark Structured
Streaming and writes to a Bronze Iceberg table with the same metadata columns
as the batch BronzePipeline:

  _ingested_at    TIMESTAMP
  _source_name    STRING
  _batch_id       STRING     — a new UUID per microbatch
  _batch_mode     STRING     — always "streaming"
  _ingestion_date DATE

The pipeline runs until cancelled (trigger=processingTime) or until all
available data is processed (trigger=once / availableNow).

Usage:
    from killuhub.layers.streaming import StreamingBronzePipeline, StreamingBronzeConfig
    from killuhub.core.batch import StreamingConfig

    config = StreamingBronzeConfig(
        engine_name="spark",
        engine_kwargs={"warehouse": "s3a://bucket/warehouse", ...},
        source_format="kafka",
        source_options={
            "kafka.bootstrap.servers": "broker:9092",
            "subscribe": "orders",
            "startingOffsets": "latest",
        },
        streaming_config=StreamingConfig(
            trigger="processingTime",
            trigger_interval="30 seconds",
            checkpoint_location="s3a://bucket/checkpoints/orders",
            output_mode="append",
        ),
        bronze_table="prod.bronze.orders",
        source_name="kafka.orders",
        partition_by=["_ingestion_date"],
    )
    StreamingBronzePipeline(config).run()
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from killuhub.core.batch import StreamingConfig
from killuhub.core.exceptions import PipelineError

logger = logging.getLogger(__name__)


@dataclass
class StreamingBronzeConfig:
    """
    Configuration for a streaming Bronze ingestion run.

    Attributes:
        engine_name:
            Processing engine — always "spark" for structured streaming.

        engine_kwargs:
            Keyword args forwarded to SparkEngine.init_session()
            (warehouse, catalog_name, catalog_type, etc.).

        source_format:
            Spark readStream format: "kafka", "json", "csv", "parquet", "delta".

        source_options:
            Options dict forwarded to readStream.options().
            For Kafka: bootstrap.servers, subscribe/subscribePattern, startingOffsets.
            For file sources: path, maxFilesPerTrigger.

        schema:
            Optional StructType schema for the input stream.
            Required for file sources; Kafka provides a fixed schema automatically
            (key, value, topic, partition, offset, timestamp).

        value_deserializer:
            Optional transform applied to parse raw Kafka value bytes into
            typed columns before metadata is stamped. Receives and returns a
            Spark DataFrame.

        streaming_config:
            Trigger type, interval, checkpoint location, output mode.

        bronze_table:
            Fully qualified target Iceberg table name.

        source_name:
            Human-readable label written to _source_name column.

        partition_by:
            Columns to partition the Bronze Iceberg table by.
            Defaults to ["_ingestion_date"].

        contract:
            Optional ContractSpec validated per microbatch.
    """
    engine_name: str
    engine_kwargs: dict[str, Any]
    source_format: str
    source_options: dict[str, Any]
    streaming_config: StreamingConfig
    bronze_table: str
    source_name: str
    schema: Any = None                      # pyspark.sql.types.StructType | None
    value_deserializer: Callable | None = None
    partition_by: list[str] = field(default_factory=lambda: ["_ingestion_date"])
    contract: Any = None                    # ContractSpec | None

    def __post_init__(self):
        if not self.bronze_table:
            raise ValueError("bronze_table must be set.")
        if not self.source_name:
            raise ValueError("source_name must be set.")
        if not self.source_options:
            raise ValueError("source_options must be set (e.g. kafka bootstrap servers).")


class StreamingBronzePipeline:
    """
    Orchestrates streaming source → Bronze Iceberg ingestion using
    Spark Structured Streaming.

    The write loop runs until the job is cancelled externally (SIGTERM from
    Spark Operator / Databricks / EMR) or until trigger=once completes.
    """

    def __init__(self, config: StreamingBronzeConfig):
        self.config = config

    def run(self) -> None:
        """
        Start the streaming pipeline. Blocks until the stream terminates
        (trigger=once/availableNow) or is cancelled externally.
        """
        cfg = self.config
        sc = cfg.streaming_config

        logger.info(
            "StreamingBronzePipeline starting | source=%s table=%s trigger=%s[%s]",
            cfg.source_name, cfg.bronze_table, sc.trigger, sc.trigger_interval,
        )

        try:
            from killuhub.processing.spark_engine import SparkEngine
        except ImportError as exc:
            raise PipelineError("SparkEngine is required for streaming.") from exc

        engine = SparkEngine()
        try:
            engine.init_session(app_name="killuhub-streaming", **cfg.engine_kwargs)
            spark = engine.spark

            # ── Build readStream ─────────────────────────────────────────────
            reader = spark.readStream.format(cfg.source_format)
            if cfg.schema is not None:
                reader = reader.schema(cfg.schema)
            reader = reader.options(**cfg.source_options)
            stream_df = reader.load()

            # ── Optional value deserialization (e.g. Kafka bytes → JSON) ────
            if cfg.value_deserializer is not None:
                stream_df = cfg.value_deserializer(stream_df)

            # ── Microbatch processor: stamp metadata per batch ───────────────
            source_name = cfg.source_name
            contract = cfg.contract
            bronze_table = cfg.bronze_table

            def process_batch(batch_df, batch_id_int: int):
                from pyspark.sql import functions as F

                batch_id = str(uuid.uuid4())
                df = (
                    batch_df
                    .withColumn("_ingested_at",    F.current_timestamp())
                    .withColumn("_source_name",    F.lit(source_name))
                    .withColumn("_batch_id",       F.lit(batch_id))
                    .withColumn("_batch_mode",     F.lit("streaming"))
                    .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))
                )

                if contract is not None:
                    from killuhub.core.contract import ContractValidator
                    ContractValidator(contract).validate(
                        df, table_name=bronze_table, run_id=batch_id
                    )

                # Write microbatch to Iceberg using foreachBatch
                (
                    df.writeTo(bronze_table)
                    .option("fanout-enabled", "true")
                    .partitionedBy(*cfg.partition_by)
                    .append()
                )
                logger.info(
                    "Microbatch %d written | batch_id=%s rows=%d",
                    batch_id_int, batch_id, df.count(),
                )

            # ── Build writeStream ────────────────────────────────────────────
            writer = (
                stream_df.writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", sc.checkpoint_location)
                .outputMode(sc.output_mode)
            )

            # Apply trigger
            if sc.trigger == "once":
                writer = writer.trigger(once=True)
            elif sc.trigger == "availableNow":
                writer = writer.trigger(availableNow=True)
            elif sc.trigger == "continuous":
                writer = writer.trigger(continuous=sc.trigger_interval)
            else:
                # processingTime (default)
                writer = writer.trigger(processingTime=sc.trigger_interval)

            query = writer.start()

            logger.info(
                "Streaming query started | id=%s name=%s",
                query.id, query.name,
            )
            query.awaitTermination()

        except Exception as exc:
            raise PipelineError(f"Streaming Bronze pipeline failed: {exc}") from exc
        finally:
            # Never stop on Databricks (shared session)
            engine.stop()
