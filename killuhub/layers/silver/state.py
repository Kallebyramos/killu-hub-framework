"""
Incremental state persistence for KilluHub Silver pipelines.

The state store tracks the last processed watermark for each
(source_table → target_table) pair so INCREMENTAL runs know
exactly where to pick up.

Two backends:
  JsonFileStateStore  — local JSON file (dev / single-process)
  IcebergStateStore   — Iceberg table (production / multi-engine)
"""
from __future__ import annotations

import json
import os
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from killuhub.core.batch import IncrementalState
from killuhub.core.exceptions import IncrementalStateError


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class IncrementalStateStore(ABC):
    """
    Interface for reading and writing incremental watermark state.

    load() returns None when no prior state exists for the given table pair,
    signalling that the pipeline should use BatchConfig.initial_watermark.
    """

    @abstractmethod
    def load(self, source_table: str, target_table: str) -> IncrementalState | None:
        """Return the stored state, or None if this pair has never been run."""

    @abstractmethod
    def save(self, state: IncrementalState) -> None:
        """Persist the state (upsert by source_table + target_table key)."""


# ---------------------------------------------------------------------------
# JSON file backend — development / single-machine use
# ---------------------------------------------------------------------------

class JsonFileStateStore(IncrementalStateStore):
    """
    Stores incremental state in a local JSON file.

    The file is a flat JSON object keyed by "source_table::target_table".
    Writes are atomic (temp-file + rename) so a crash mid-write cannot
    corrupt existing state.

    Args:
        path: Path to the JSON state file. Created on first save.
    """

    def __init__(self, path: str = ".killuhub_state.json"):
        self._path = path

    def load(self, source_table: str, target_table: str) -> IncrementalState | None:
        if not os.path.exists(self._path):
            return None

        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data: dict[str, Any] = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            raise IncrementalStateError(
                f"Could not read state file '{self._path}': {exc}"
            ) from exc

        key = f"{source_table}::{target_table}"
        entry = data.get(key)
        if entry is None:
            return None

        return IncrementalState(
            source_table=entry["source_table"],
            target_table=entry["target_table"],
            last_watermark=entry["last_watermark"],
            updated_at=entry.get("updated_at", ""),
        )

    def save(self, state: IncrementalState) -> None:
        # Load existing state (or start fresh)
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data: dict[str, Any] = json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                raise IncrementalStateError(
                    f"Could not read state file '{self._path}': {exc}"
                ) from exc
        else:
            data = {}

        state.updated_at = _now_iso()
        data[state.state_key()] = {
            "source_table": state.source_table,
            "target_table": state.target_table,
            "last_watermark": state.last_watermark,
            "updated_at": state.updated_at,
        }

        # Atomic write: write to temp file then rename
        dir_name = os.path.dirname(os.path.abspath(self._path))
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", dir=dir_name, delete=False, suffix=".tmp", encoding="utf-8"
            ) as tmp:
                json.dump(data, tmp, indent=2)
                tmp_path = tmp.name
            os.replace(tmp_path, self._path)
        except OSError as exc:
            raise IncrementalStateError(
                f"Could not write state file '{self._path}': {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Iceberg backend — production / multi-engine use
# ---------------------------------------------------------------------------

class IcebergStateStore(IncrementalStateStore):
    """
    Stores incremental state in an Iceberg table.

    The state table is created automatically on first save.
    State rows are upserted using Iceberg's MERGE INTO so each
    (source_table, target_table) pair always has exactly one row.

    This backend is safe across multiple processes and engines because
    Iceberg provides ACID guarantees on the MERGE.

    Args:
        spark:        An active SparkSession with Iceberg extensions enabled.
        state_table:  Fully qualified Iceberg table name.
        catalog_name: Catalog name (used for CALL system procedures).
    """

    _CREATE_DDL = """
        CREATE TABLE IF NOT EXISTS {table} (
            source_table   STRING NOT NULL,
            target_table   STRING NOT NULL,
            last_watermark STRING NOT NULL,
            updated_at     STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read'
        )
    """

    def __init__(
        self,
        spark: Any,
        state_table: str = "local.meta.killuhub_state",
        catalog_name: str = "local",
    ):
        self._spark = spark
        self._table = state_table
        self._catalog = catalog_name
        self._ensured = False

    def _ensure_table(self) -> None:
        if self._ensured:
            return
        try:
            self._spark.sql(self._CREATE_DDL.format(table=self._table))
            self._ensured = True
        except Exception as exc:
            raise IncrementalStateError(
                f"Could not create state table '{self._table}': {exc}"
            ) from exc

    def load(self, source_table: str, target_table: str) -> IncrementalState | None:
        self._ensure_table()
        try:
            rows = self._spark.sql(f"""
                SELECT source_table, target_table, last_watermark, updated_at
                FROM   {self._table}
                WHERE  source_table = '{source_table}'
                AND    target_table = '{target_table}'
                LIMIT 1
            """).collect()
        except Exception as exc:
            raise IncrementalStateError(
                f"Could not query state table '{self._table}': {exc}"
            ) from exc

        if not rows:
            return None

        row = rows[0]
        return IncrementalState(
            source_table=row["source_table"],
            target_table=row["target_table"],
            last_watermark=row["last_watermark"],
            updated_at=row["updated_at"] or "",
        )

    def save(self, state: IncrementalState) -> None:
        self._ensure_table()
        state.updated_at = _now_iso()
        try:
            self._spark.sql(f"""
                MERGE INTO {self._table} AS target
                USING (
                    SELECT
                        '{state.source_table}'  AS source_table,
                        '{state.target_table}'  AS target_table,
                        '{state.last_watermark}' AS last_watermark,
                        '{state.updated_at}'    AS updated_at
                ) AS source
                ON target.source_table = source.source_table
                AND target.target_table = source.target_table
                WHEN MATCHED THEN
                    UPDATE SET
                        last_watermark = source.last_watermark,
                        updated_at     = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (source_table, target_table, last_watermark, updated_at)
                    VALUES (source.source_table, source.target_table,
                            source.last_watermark, source.updated_at)
            """)
        except Exception as exc:
            raise IncrementalStateError(
                f"Could not save state to '{self._table}': {exc}"
            ) from exc
