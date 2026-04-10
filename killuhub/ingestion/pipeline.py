"""
KilluHub ingestion pipeline.

The Pipeline ties everything together:
  Connector  →  batch records  →  Engine (Spark/Flink)  →  transform  →  StorageWriter

Usage:
    config = PipelineConfig(
        connector_name="postgres",
        connector_config=ConnectorConfig.from_dict({...}),
        engine_name="spark",
        storage_writer_name="iceberg",
        target_table="local.db.orders",
        write_mode="append",
        batch_size=10_000,
        transform_fn=lambda df: df.filter("amount > 0"),
        engine_kwargs={"warehouse": "s3://my-bucket/warehouse"},
        storage_kwargs={"partition_by": ["event_date"]},
    )
    Pipeline(config).run()
"""
import logging
from itertools import islice
from typing import Any

from killuhub.core.config import PipelineConfig
from killuhub.core.registry import default_registry
from killuhub.storage.iceberg.writer import IcebergWriter
from killuhub.storage.delta.writer import DeltaWriter
from killuhub.storage.hudi.writer import HudiWriter
from killuhub.core.exceptions import PipelineError

logger = logging.getLogger(__name__)

# Map writer names → classes (these need the spark session at instantiation)
_WRITER_CLASSES = {
    "iceberg": IcebergWriter,
    "delta": DeltaWriter,
    "hudi": HudiWriter,
}


class Pipeline:
    """
    Orchestrates a full extract → process → load cycle.

    The pipeline streams records from the connector in batches,
    converts each batch to an engine DataFrame, applies the optional
    transform, and writes the result to the configured storage writer.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._connector = None
        self._engine = None
        self._writer = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        """
        Execute the pipeline end-to-end.

        Returns a summary dict with total_records and total_batches.
        """
        logger.info(
            "Starting pipeline | connector=%s engine=%s writer=%s table=%s",
            self.config.connector_name,
            self.config.engine_name,
            self.config.storage_writer_name,
            self.config.target_table,
        )

        total_records = 0
        total_batches = 0

        try:
            connector_cls = default_registry.get_connector(self.config.connector_name)
            engine_cls = default_registry.get_engine(self.config.engine_name)

            self._connector = connector_cls(self.config.connector_config)
            self._engine = engine_cls()
            self._engine.init_session(
                app_name=f"killuhub-{self.config.connector_name}",
                **self.config.engine_kwargs,
            )
            self._writer = self._build_writer()

            with self._connector:
                records_iter = self._connector.extract()
                while True:
                    batch = list(islice(records_iter, self.config.batch_size))
                    if not batch:
                        break

                    df = self._engine.create_dataframe(batch)
                    df = self._engine.apply_transform(df, self.config.transform_fn)
                    self._writer.write(df, self.config.target_table, self.config.write_mode)

                    total_records += len(batch)
                    total_batches += 1
                    logger.info(
                        "Batch %d written | records_in_batch=%d total=%d",
                        total_batches, len(batch), total_records,
                    )

        except Exception as exc:
            raise PipelineError(f"Pipeline failed: {exc}") from exc
        finally:
            if self._engine:
                self._engine.stop()

        summary = {
            "connector": self.config.connector_name,
            "engine": self.config.engine_name,
            "writer": self.config.storage_writer_name,
            "target_table": self.config.target_table,
            "total_records": total_records,
            "total_batches": total_batches,
        }
        logger.info("Pipeline complete | %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_writer(self):
        writer_name = self.config.storage_writer_name
        if writer_name not in _WRITER_CLASSES:
            raise PipelineError(
                f"Unknown storage writer '{writer_name}'. "
                f"Available: {list(_WRITER_CLASSES)}"
            )
        # Writers need the engine's native session (e.g. SparkSession)
        session = getattr(self._engine, "spark", None) or getattr(
            self._engine, "table_env", None
        )
        return _WRITER_CLASSES[writer_name](session, **self.config.storage_kwargs)
