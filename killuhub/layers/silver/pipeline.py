"""
Silver layer pipeline for KilluHub.

Reads from a Bronze Iceberg table, applies standard + user-defined
transformations, and writes to a Silver Iceberg table.

Supports two batch modes:
  FULL        — reads all records from Bronze and overwrites Silver
  INCREMENTAL — reads only records newer than the last stored watermark
                and appends them to Silver

Usage:
    from killuhub.layers.silver import SilverPipeline, SilverConfig
    from killuhub.core.batch import BatchMode, BatchConfig

    config = SilverConfig(
        bronze_table="local.bronze.orders",
        silver_table="local.silver.orders",
        batch_config=BatchConfig(
            mode=BatchMode.INCREMENTAL,
            watermark_column="_ingested_at",
        ),
        key_columns=["order_id"],
        date_columns=["created_at", "updated_at"],
        type_map={"amount": "double", "quantity": "int"},
        partition_by=["order_date"],
        engine_kwargs={"warehouse": "/tmp/killuhub-warehouse"},
    )
    SilverPipeline(config).run()
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from killuhub.core.batch import BatchConfig, BatchMode, IncrementalState
from killuhub.core.exceptions import PipelineError
from killuhub.layers.silver.state import (
    IncrementalStateStore,
    JsonFileStateStore,
)
from killuhub.layers.silver.transformations import (
    add_metadata_columns,
    add_date_dimensions,
    apply_transforms,
    cast_types,
    deduplicate,
    parse_date_columns,
)

logger = logging.getLogger(__name__)


@dataclass
class SilverConfig:
    """
    Configuration for a Silver pipeline run.

    Attributes:
        bronze_table:       Fully qualified source Bronze Iceberg table.
        silver_table:       Fully qualified target Silver Iceberg table.
        batch_config:       BatchMode (FULL/INCREMENTAL) + watermark settings.

        watermark_column:   Column to filter on in INCREMENTAL mode.
                            Defaults to `_ingested_at` — the metadata column
                            that Bronze stamps, so Silver always works
                            regardless of source schema.

        key_columns:        Unique key columns for deduplication.
                            If empty, deduplication is skipped.

        date_columns:       String columns to parse to TimestampType.
        date_dimension_column:
                            If set, `add_date_dimensions()` is called on this
                            column to add year/month/day/hour/date columns.

        type_map:           Column → Spark SQL type for casting.
        rename_map:         Column rename pairs applied before writing.
        drop_columns:       Columns to drop before writing.
        null_check_columns: Rows with nulls in these columns are dropped.

        user_transform_fn:  Optional user-supplied DataFrame → DataFrame,
                            applied after all standard transforms.

        partition_by:       Silver table partition columns.
        state_store:        Injected state store; defaults to JsonFileStateStore.
        engine_kwargs:      Forwarded to SparkEngine.init_session().
        storage_kwargs:     Forwarded to IcebergWriter.
    """
    bronze_table: str
    silver_table: str
    batch_config: BatchConfig
    watermark_column: str = "_ingested_at"
    key_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    date_dimension_column: str = ""
    type_map: dict[str, str] = field(default_factory=dict)
    rename_map: dict[str, str] = field(default_factory=dict)
    drop_columns: list[str] = field(default_factory=list)
    null_check_columns: list[str] = field(default_factory=list)
    user_transform_fn: Any = None
    partition_by: list[str] = field(default_factory=list)
    state_store: IncrementalStateStore | None = None
    engine_kwargs: dict[str, Any] = field(default_factory=dict)
    storage_kwargs: dict[str, Any] = field(default_factory=dict)
    contract: Any = None   # ContractSpec | None — validated after all transforms, before write


class SilverPipeline:
    """
    Orchestrates a Bronze → Silver transformation run.

    Does not use a connector — data is read directly from the Bronze
    Iceberg table via SparkSession, which is the most efficient path
    for Iceberg-to-Iceberg processing.
    """

    def __init__(self, config: SilverConfig):
        self.config = config
        self._run_id = str(uuid.uuid4())

    def run(self) -> dict[str, Any]:
        """
        Execute the Bronze → Silver pipeline.

        Returns a summary dict with run metadata and record counts.
        """
        cfg = self.config
        run_id = self._run_id
        mode = cfg.batch_config.mode

        logger.info(
            "Silver pipeline starting | run_id=%s mode=%s bronze=%s silver=%s",
            run_id, mode.value, cfg.bronze_table, cfg.silver_table,
        )

        engine = None
        try:
            engine = self._start_engine()
            spark = engine.spark

            state_store = cfg.state_store or JsonFileStateStore()

            # ----------------------------------------------------------------
            # 1. Resolve watermark for INCREMENTAL mode
            # ----------------------------------------------------------------
            last_watermark = self._resolve_watermark(state_store)

            # ----------------------------------------------------------------
            # 2. Read from Bronze
            # ----------------------------------------------------------------
            df = self._read_bronze(spark, mode, last_watermark)

            if df.rdd.isEmpty():
                logger.info(
                    "Silver pipeline | run_id=%s | no new records in Bronze — skipping.",
                    run_id,
                )
                return self._summary(run_id, mode, 0, skipped=True)

            input_count = df.count()
            logger.info(
                "Silver pipeline | run_id=%s | read %d records from Bronze.",
                run_id, input_count,
            )

            # ----------------------------------------------------------------
            # 3. Apply transforms
            # ----------------------------------------------------------------
            df = self._apply_all_transforms(df, run_id)

            output_count = df.count()

            # ----------------------------------------------------------------
            # 4. Contract validation (after transforms, before write)
            # ----------------------------------------------------------------
            contract_report = None
            if cfg.contract is not None:
                from killuhub.core.contract import ContractValidator
                contract_report = ContractValidator(cfg.contract).validate(
                    df, table_name=cfg.silver_table, run_id=run_id
                )

            # ----------------------------------------------------------------
            # 5. Write to Silver
            # ----------------------------------------------------------------
            write_mode = "overwrite" if mode == BatchMode.FULL else "append"
            writer = self._build_writer(spark)
            writer.write(df, cfg.silver_table, mode=write_mode)

            logger.info(
                "Silver pipeline | run_id=%s | wrote %d records to %s (mode=%s).",
                run_id, output_count, cfg.silver_table, write_mode,
            )

            # ----------------------------------------------------------------
            # 5. Update watermark state
            # ----------------------------------------------------------------
            new_watermark = self._compute_new_watermark(df, spark)
            self._save_state(state_store, new_watermark)

            summary = self._summary(run_id, mode, output_count, contract_report)
            logger.info("Silver pipeline complete | %s", summary)
            return summary

        except Exception as exc:
            raise PipelineError(f"Silver pipeline failed: {exc}") from exc
        finally:
            if engine:
                engine.stop()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _start_engine(self):
        from killuhub.processing.spark_engine import SparkEngine

        engine = SparkEngine()
        engine.init_session(
            app_name=f"killuhub-silver-{self.config.silver_table}",
            **self.config.engine_kwargs,
        )
        return engine

    def _resolve_watermark(self, state_store: IncrementalStateStore) -> str:
        cfg = self.config
        if cfg.batch_config.mode == BatchMode.FULL:
            return cfg.batch_config.initial_watermark  # unused in FULL, returned for symmetry

        prior_state = state_store.load(cfg.bronze_table, cfg.silver_table)
        watermark = (
            prior_state.last_watermark
            if prior_state
            else cfg.batch_config.initial_watermark
        )
        logger.info(
            "Silver pipeline | INCREMENTAL watermark = %s (column: %s)",
            watermark, cfg.watermark_column,
        )
        return watermark

    def _read_bronze(self, spark, mode: BatchMode, last_watermark: str):
        cfg = self.config
        if mode == BatchMode.FULL:
            logger.info("Silver pipeline | FULL read from %s", cfg.bronze_table)
            return spark.table(cfg.bronze_table)

        # INCREMENTAL: filter on watermark column
        # ORDER BY ensures deterministic processing if the run is retried
        logger.info(
            "Silver pipeline | INCREMENTAL read: %s > '%s'",
            cfg.watermark_column, last_watermark,
        )
        return spark.sql(f"""
            SELECT *
            FROM   {cfg.bronze_table}
            WHERE  {cfg.watermark_column} > TIMESTAMP '{last_watermark}'
            ORDER BY {cfg.watermark_column} ASC
        """)

    def _apply_all_transforms(self, df, run_id: str):
        from killuhub.layers.silver.transformations import (
            rename_columns,
            drop_columns as drop_cols,
            filter_nulls,
        )

        cfg = self.config

        steps: list = [
            # Stamp Silver metadata
            lambda d: add_metadata_columns(d, layer="silver", run_id=run_id),

            # Parse string → timestamp
            (lambda d: parse_date_columns(d, cfg.date_columns))
            if cfg.date_columns else None,

            # Add year/month/day/hour/date columns
            (lambda d: add_date_dimensions(d, cfg.date_dimension_column))
            if cfg.date_dimension_column else None,

            # Cast column types
            (lambda d: cast_types(d, cfg.type_map))
            if cfg.type_map else None,

            # Rename columns
            (lambda d: rename_columns(d, cfg.rename_map))
            if cfg.rename_map else None,

            # Drop unwanted columns
            (lambda d: drop_cols(d, cfg.drop_columns))
            if cfg.drop_columns else None,

            # Drop rows with nulls in key columns
            (lambda d: filter_nulls(d, cfg.null_check_columns))
            if cfg.null_check_columns else None,

            # Deduplication
            (lambda d: deduplicate(d, cfg.key_columns))
            if cfg.key_columns else None,

            # User-supplied transform (applied last)
            cfg.user_transform_fn,
        ]

        return apply_transforms(df, steps)

    def _build_writer(self, spark):
        from killuhub.storage.iceberg.writer import IcebergWriter

        return IcebergWriter(
            spark,
            partition_by=self.config.partition_by,
            **self.config.storage_kwargs,
        )

    def _compute_new_watermark(self, df, spark) -> str:
        """
        Find the maximum value of watermark_column in the processed batch.
        Falls back to current UTC time if the column isn't present or is all-null.
        """
        col = self.config.watermark_column
        if col not in df.columns:
            return datetime.now(timezone.utc).isoformat()

        from pyspark.sql import functions as F
        result = df.agg(F.max(col).alias("max_wm")).collect()
        max_val = result[0]["max_wm"] if result else None

        if max_val is None:
            return datetime.now(timezone.utc).isoformat()

        # Handle both datetime objects and strings
        if isinstance(max_val, datetime):
            return max_val.isoformat()
        return str(max_val)

    def _save_state(self, state_store: IncrementalStateStore, new_watermark: str) -> None:
        cfg = self.config
        state = IncrementalState(
            source_table=cfg.bronze_table,
            target_table=cfg.silver_table,
            last_watermark=new_watermark,
        )
        state_store.save(state)
        logger.info(
            "Silver pipeline | watermark updated to %s for %s → %s",
            new_watermark, cfg.bronze_table, cfg.silver_table,
        )

    def _summary(
        self,
        run_id: str,
        mode: BatchMode,
        records_written: int,
        contract_report=None,
        skipped: bool = False,
    ) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "run_id": run_id,
            "mode": mode.value,
            "bronze_table": self.config.bronze_table,
            "silver_table": self.config.silver_table,
            "records_written": records_written,
            "skipped": skipped,
        }
        if contract_report is not None:
            summary["contract"] = contract_report.to_dict()
        return summary
