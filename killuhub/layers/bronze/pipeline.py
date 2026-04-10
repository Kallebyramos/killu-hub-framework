"""
Bronze layer pipeline for KilluHub.

Ingests raw data from any registered connector and writes it to a
Bronze Iceberg table with four standard metadata columns appended to
every row:

  _ingested_at   TIMESTAMP  — when this batch was written
  _source_name   STRING     — human-readable source label
  _batch_id      STRING     — UUID for this specific run
  _batch_mode    STRING     — "full" or "incremental"
  _ingestion_date DATE      — date-only truncation (used as partition column)

Bronze is always APPEND — historical data is never modified or deleted.
The _batch_id column makes every write idempotent: re-running the same
logical batch (same batch_id) can be detected and deduplicated downstream
in the Silver layer.

Usage:
    from killuhub.layers.bronze import BronzePipeline, BronzeConfig
    from killuhub.core.batch import BatchMode, BatchConfig

    config = BronzeConfig(
        pipeline_config=PipelineConfig(
            connector_name="postgres",
            connector_config=ConnectorConfig.from_dict({...}),
            engine_name="spark",
            engine_kwargs={"warehouse": "/tmp/killuhub"},
        ),
        batch_config=BatchConfig(mode=BatchMode.FULL),
        bronze_table="local.bronze.orders",
        source_name="postgres.shop.orders",
    )
    BronzePipeline(config).run()
"""
from __future__ import annotations

import logging
import uuid
from copy import copy
from dataclasses import dataclass, field
from typing import Any

from killuhub.core.batch import BatchConfig, BatchMode
from killuhub.core.config import PipelineConfig
from killuhub.core.exceptions import PipelineError

logger = logging.getLogger(__name__)


@dataclass
class BronzeConfig:
    """
    Configuration for a Bronze ingestion run.

    Attributes:
        pipeline_config:  The underlying connector/engine config.
                          `target_table` and `write_mode` in this config
                          are ignored — Bronze sets them automatically.

        batch_config:     BatchMode and watermark parameters.
                          Bronze itself does not filter records — the
                          connector's query is responsible for that.
                          The mode is stamped as metadata for lineage.

        bronze_table:     Fully qualified target Iceberg table name.
                          e.g. "local.bronze.orders"

        source_name:      Human-readable source label written into
                          the `_source_name` metadata column.
                          e.g. "postgres.shop.orders"

        partition_column: Column name to partition the Bronze table by.
                          Defaults to "_ingestion_date" (the date metadata
                          column added automatically by Bronze).

        contract:         Optional data contract. When set, the contract
                          is validated against the raw data AFTER metadata
                          columns are added but BEFORE writing to Iceberg.
                          Use this to catch source-level quality issues early.
    """
    pipeline_config: PipelineConfig
    batch_config: BatchConfig
    bronze_table: str
    source_name: str
    partition_column: str = "_ingestion_date"
    contract: Any = None   # ContractSpec | None

    def __post_init__(self):
        if not self.bronze_table:
            raise ValueError("bronze_table must be set.")
        if not self.source_name:
            raise ValueError("source_name must be set.")


class BronzePipeline:
    """
    Orchestrates source → Bronze Iceberg ingestion.

    Composes the existing Pipeline rather than extending it, so all
    connector/engine/writer logic stays in one place and BronzePipeline
    only handles the metadata injection concern.
    """

    def __init__(self, config: BronzeConfig):
        self.config = config

    def run(self) -> dict[str, Any]:
        """
        Execute the Bronze ingestion.

        Returns the Pipeline summary dict augmented with batch_id and mode.
        """
        cfg = self.config

        # Resolve batch_id: use configured value or generate a fresh UUID
        batch_id = cfg.batch_config.batch_id or str(uuid.uuid4())
        mode_value = cfg.batch_config.mode.value

        logger.info(
            "Bronze pipeline starting | batch_id=%s mode=%s source=%s table=%s",
            batch_id, mode_value, cfg.source_name, cfg.bronze_table,
        )

        # Build the metadata injector that wraps any existing transform
        original_transform = cfg.pipeline_config.transform_fn
        bronze_transform = self._make_metadata_transform(
            batch_id=batch_id,
            mode_value=mode_value,
            source_name=cfg.source_name,
            original_transform=original_transform,
            contract=cfg.contract,
            bronze_table=cfg.bronze_table,
        )

        # Build a modified PipelineConfig: override target, mode, transform
        modified_storage_kwargs = dict(cfg.pipeline_config.storage_kwargs)
        modified_storage_kwargs["partition_by"] = [cfg.partition_column]

        modified_config = PipelineConfig(
            connector_name=cfg.pipeline_config.connector_name,
            connector_config=cfg.pipeline_config.connector_config,
            engine_name=cfg.pipeline_config.engine_name,
            storage_writer_name=cfg.pipeline_config.storage_writer_name,
            target_table=cfg.bronze_table,
            write_mode="append",           # Bronze is always append
            batch_size=cfg.pipeline_config.batch_size,
            transform_fn=bronze_transform,
            engine_kwargs=cfg.pipeline_config.engine_kwargs,
            storage_kwargs=modified_storage_kwargs,
        )

        try:
            from killuhub.ingestion.pipeline import Pipeline
            summary = Pipeline(modified_config).run()
        except Exception as exc:
            raise PipelineError(f"Bronze pipeline failed: {exc}") from exc

        summary["batch_id"] = batch_id
        summary["batch_mode"] = mode_value

        logger.info("Bronze pipeline complete | %s", summary)
        return summary

    # ------------------------------------------------------------------

    @staticmethod
    def _make_metadata_transform(
        batch_id: str,
        mode_value: str,
        source_name: str,
        original_transform,
        contract=None,
        bronze_table: str = "",
    ):
        """
        Return a Spark transform that:
        1. Applies the original user transform (if any).
        2. Stamps the five Bronze metadata columns onto every row.
        """
        def _transform(df):
            from pyspark.sql import functions as F

            # Apply user's transform first (e.g., basic filtering before bronze)
            if original_transform is not None:
                df = original_transform(df)

            df = (
                df
                .withColumn("_ingested_at",   F.current_timestamp())
                .withColumn("_source_name",   F.lit(source_name))
                .withColumn("_batch_id",      F.lit(batch_id))
                .withColumn("_batch_mode",    F.lit(mode_value))
                .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))
            )

            # Run contract validation if configured
            if contract is not None:
                from killuhub.core.contract import ContractValidator
                ContractValidator(contract).validate(
                    df, table_name=bronze_table, run_id=batch_id
                )

            return df

        return _transform
