"""
Batch and streaming processing primitives for KilluHub.

ExecutionMode controls whether a pipeline runs as a one-shot batch job or as a
continuous stream. Within batch mode, BatchMode controls whether all records are
processed (FULL) or only records added since the last run (INCREMENTAL).

These are core-layer types — no Spark, no I/O, no external dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ExecutionMode(str, Enum):
    """
    Top-level pipeline execution mode.

    BATCH     — one-shot job: read, transform, write, exit.
    STREAMING — continuous job: run indefinitely, processing records as they
                arrive (Spark Structured Streaming or Flink).
    """
    BATCH     = "batch"
    STREAMING = "streaming"


class BatchMode(str, Enum):
    """
    Strategy for a BATCH execution run.

    Using `str` as a mixin base means `BatchMode.FULL == "full"` is True
    and the value serialises to a plain string in JSON state files without
    a custom encoder.

    FULL        — read/process all available records.
    INCREMENTAL — read only records newer than the last watermark cursor.
    """
    FULL        = "full"
    INCREMENTAL = "incremental"


@dataclass
class BatchConfig:
    """
    Batch execution parameters — only used when ExecutionMode is BATCH.

    Attributes:
        mode:
            FULL — read/process all available records.
            INCREMENTAL — read only records newer than the last watermark.

        watermark_column:
            Column used to advance the incremental cursor.
            For Bronze this is typically the source's own timestamp column
            (e.g. `updated_at`).
            For Silver this is typically `_ingested_at` (the Bronze metadata
            column) so Silver can filter on a column it controls regardless
            of the source schema.

        initial_watermark:
            ISO-8601 string used as the lower bound on the very first
            INCREMENTAL run when no prior state exists.
            Default epoch means "process everything on first run".

        batch_id:
            Unique identifier for this run, written to the `_batch_id`
            metadata column in Bronze. If empty, Bronze auto-generates a UUID
            at runtime. Injecting a deterministic ID makes reruns idempotent.
    """
    mode: BatchMode
    watermark_column: str = "updated_at"
    initial_watermark: str = "1970-01-01T00:00:00"
    batch_id: str = ""


@dataclass
class StreamingConfig:
    """
    Streaming execution parameters — only used when ExecutionMode is STREAMING.

    Attributes:
        trigger:
            Spark Structured Streaming trigger type.
            processingTime  — microbatch, runs every `trigger_interval`.
            once            — processes all available data in one microbatch then stops.
            availableNow    — like once but respects rate limits.
            continuous      — experimental low-latency continuous processing.

        trigger_interval:
            Microbatch interval string (e.g. "30 seconds", "1 minute").
            Only used when trigger is processingTime.

        checkpoint_location:
            Path where Spark stores streaming checkpoint state.
            Must be durable storage (S3, HDFS, ADLS) in production.
            Required for exactly-once guarantees and job restarts.

        output_mode:
            append   — only new rows written each microbatch (default, Iceberg).
            update   — rows that changed are written (requires watermarking).
            complete — full result written each time (for aggregations).
    """
    trigger: str = "processingTime"
    trigger_interval: str = "30 seconds"
    checkpoint_location: str = "/tmp/killuhub-checkpoints"
    output_mode: str = "append"


@dataclass
class IncrementalState:
    """
    Persisted watermark state for an INCREMENTAL Silver run.

    Each (source_table, target_table) pair has one state record.
    After a successful Silver run the `last_watermark` is updated to
    max(watermark_column) observed in that batch, so the next run
    starts exactly from where this one ended.

    Attributes:
        source_table:    Fully qualified bronze Iceberg table name.
        target_table:    Fully qualified silver Iceberg table name.
        last_watermark:  ISO-8601 string. Next incremental run will
                         filter `watermark_column > last_watermark`.
        updated_at:      ISO-8601 string of when this state was written.
                         Informational — not used for query filtering.
    """
    source_table: str
    target_table: str
    last_watermark: str
    updated_at: str = field(default="")

    def state_key(self) -> str:
        """Stable dict key for JSON serialisation."""
        return f"{self.source_table}::{self.target_table}"
