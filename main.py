"""
KilluHub — main entry point.

Reads a YAML pipeline config, resolves environment variables, and
dispatches to the correct pipeline type (bronze, silver, or chain).

This file is designed to be the `mainApplicationFile` when submitted
via Spark Operator on Kubernetes.

Usage:
  Local:
    python main.py --config config/bronze_postgres.yaml

  Spark-submit:
    spark-submit main.py --config /etc/killuhub/pipeline.yaml

  Spark Operator (K8s):
    mainApplicationFile: "local:///app/main.py"
    arguments: ["--config", "/etc/killuhub/pipeline.yaml"]

  Environment variable config (useful in K8s when config is injected):
    KILLUHUB_CONFIG=/etc/killuhub/pipeline.yaml python main.py

Environment variable substitution in YAML:
  ${VAR}          → value of VAR (error if not set)
  ${VAR:-default} → value of VAR, or "default" if not set
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Logging setup — stdout so K8s captures it
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("killuhub.main")


# ---------------------------------------------------------------------------
# YAML loading with env var substitution
# ---------------------------------------------------------------------------

_ENV_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-(.*?))?\}")


def _substitute_env_vars(text: str) -> str:
    """
    Replace ${VAR} and ${VAR:-default} patterns with environment variable values.

    Raises:
        EnvironmentError: if a required variable (no default) is not set.
    """
    def replace(match: re.Match) -> str:
        var_name = match.group(1)
        default  = match.group(2)   # None if no :- was given
        value    = os.environ.get(var_name)
        if value is not None:
            return value
        if default is not None:
            return default
        raise EnvironmentError(
            f"Required environment variable '{var_name}' is not set "
            f"and has no default in the config file."
        )

    return _ENV_VAR_RE.sub(replace, text)


def load_config(path: str) -> dict[str, Any]:
    """
    Load and parse a YAML config file with env var substitution.

    Args:
        path: Absolute or relative path to the YAML file.

    Returns:
        Parsed config dict.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: '{path}'")

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    substituted = _substitute_env_vars(raw)

    try:
        return yaml.safe_load(substituted)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in '{path}': {exc}") from exc


# ---------------------------------------------------------------------------
# Pipeline builders
# ---------------------------------------------------------------------------

def _build_bronze(cfg: dict) -> Any:
    from killuhub import BronzeConfig, PipelineConfig, ConnectorConfig, BatchConfig
    from killuhub.core.batch import BatchMode
    from killuhub.core.contract import ContractSpec

    batch_raw  = cfg.get("batch", {})
    engine_raw = cfg.get("engine", {})
    bronze_raw = cfg.get("bronze", {})
    conn_raw   = cfg.get("connector", {})

    engine_kwargs = {
        "warehouse":    engine_raw.get("warehouse", "/tmp/killuhub-warehouse"),
        "catalog_name": engine_raw.get("catalog_name", "local"),
        "catalog_type": engine_raw.get("catalog_type", "hadoop"),
    }
    if "unity_catalog" in engine_raw:
        engine_kwargs["unity_catalog"] = engine_raw["unity_catalog"]
    if "extra_config" in engine_raw:
        engine_kwargs["extra_config"] = engine_raw["extra_config"]

    contract = None
    if cfg.get("contract"):
        contract = ContractSpec.from_dict(cfg["contract"])

    # `batch.strategy` is the canonical key; fall back to `batch.mode` for
    # backwards compatibility with older config files.
    strategy = batch_raw.get("strategy") or batch_raw.get("mode", "full")

    # `bronze.partition_by` (list) supersedes the old `bronze.partition_column`
    partition_by = bronze_raw.get("partition_by") or [
        bronze_raw.get("partition_column", "_ingestion_date")
    ]

    return BronzeConfig(
        pipeline_config=PipelineConfig(
            connector_name=conn_raw["name"],
            connector_config=ConnectorConfig.from_dict(conn_raw.get("config", {})),
            engine_name=engine_raw.get("name", "spark"),
            batch_size=cfg.get("batch_size", 10_000),
            engine_kwargs=engine_kwargs,
        ),
        batch_config=BatchConfig(
            mode=BatchMode(strategy),
            watermark_column=batch_raw.get("watermark_column", "updated_at"),
            initial_watermark=batch_raw.get("initial_watermark", "1970-01-01T00:00:00"),
            batch_id=batch_raw.get("batch_id", ""),
        ),
        bronze_table=bronze_raw["table"],
        source_name=bronze_raw.get("source_name", conn_raw["name"]),
        partition_column=partition_by[0],
        contract=contract,
    )


def _build_silver(cfg: dict) -> Any:
    from killuhub import SilverConfig, BatchConfig
    from killuhub.core.batch import BatchMode
    from killuhub.core.contract import ContractSpec

    batch_raw  = cfg.get("batch", {})
    engine_raw = cfg.get("engine", {})
    silver_raw = cfg.get("silver", {})

    engine_kwargs = {
        "warehouse":    engine_raw.get("warehouse", "/tmp/killuhub-warehouse"),
        "catalog_name": engine_raw.get("catalog_name", "local"),
        "catalog_type": engine_raw.get("catalog_type", "hadoop"),
    }
    if "unity_catalog" in engine_raw:
        engine_kwargs["unity_catalog"] = engine_raw["unity_catalog"]

    contract = None
    if cfg.get("contract"):
        contract = ContractSpec.from_dict(cfg["contract"])

    # State store — default JsonFile unless iceberg_state_table is set
    state_store = None
    if silver_raw.get("state_store") == "iceberg":
        state_store = "__iceberg__"

    # `batch.strategy` is canonical; fall back to `batch.mode` for compat
    strategy = batch_raw.get("strategy") or batch_raw.get("mode", "incremental")

    return SilverConfig(
        bronze_table=silver_raw["bronze_table"],
        silver_table=silver_raw["silver_table"],
        batch_config=BatchConfig(
            mode=BatchMode(strategy),
            watermark_column=batch_raw.get("watermark_column", "_ingested_at"),
            initial_watermark=batch_raw.get("initial_watermark", "1970-01-01T00:00:00"),
        ),
        watermark_column=batch_raw.get("watermark_column", "_ingested_at"),
        key_columns=silver_raw.get("key_columns", []),
        date_columns=silver_raw.get("date_columns", []),
        date_dimension_column=silver_raw.get("date_dimension_column", ""),
        type_map=silver_raw.get("type_map", {}),
        rename_map=silver_raw.get("rename_map", {}),
        drop_columns=silver_raw.get("drop_columns", []),
        null_check_columns=silver_raw.get("null_check_columns", []),
        partition_by=silver_raw.get("partition_by", []),
        state_store=state_store,
        engine_kwargs=engine_kwargs,
        storage_kwargs={"catalog_name": engine_raw.get("catalog_name", "local")},
        contract=contract,
    )


def _build_streaming_bronze(cfg: dict) -> Any:
    from killuhub.layers.streaming.pipeline import StreamingBronzeConfig
    from killuhub.core.batch import StreamingConfig
    from killuhub.core.contract import ContractSpec

    engine_raw    = cfg.get("engine", {})
    connector_raw = cfg.get("connector", {})
    bronze_raw    = cfg.get("bronze", {})
    streaming_raw = cfg.get("streaming", {})

    engine_kwargs = {
        "warehouse":    engine_raw.get("warehouse", "/tmp/killuhub-warehouse"),
        "catalog_name": engine_raw.get("catalog_name", "local"),
        "catalog_type": engine_raw.get("catalog_type", "hadoop"),
    }
    if "unity_catalog" in engine_raw:
        engine_kwargs["unity_catalog"] = engine_raw["unity_catalog"]

    contract = None
    if cfg.get("contract"):
        contract = ContractSpec.from_dict(cfg["contract"])

    partition_by = bronze_raw.get("partition_by", ["_ingestion_date"])

    return StreamingBronzeConfig(
        engine_name=engine_raw.get("name", "spark"),
        engine_kwargs=engine_kwargs,
        source_format=connector_raw.get("stream_format", "kafka"),
        source_options=connector_raw.get("stream_options", {}),
        streaming_config=StreamingConfig(
            trigger=streaming_raw.get("trigger", "processingTime"),
            trigger_interval=streaming_raw.get("trigger_interval", "30 seconds"),
            checkpoint_location=streaming_raw.get(
                "checkpoint_location", "/tmp/killuhub-checkpoints"
            ),
            output_mode=streaming_raw.get("output_mode", "append"),
        ),
        bronze_table=bronze_raw["table"],
        source_name=bronze_raw.get("source_name", connector_raw.get("name", "stream")),
        partition_by=partition_by,
        contract=contract,
    )



# ---------------------------------------------------------------------------
# Silver state store resolution (IcebergStateStore needs a live spark session)
# ---------------------------------------------------------------------------

def _resolve_silver_state_store(silver_config, spark):
    from killuhub.layers.silver.state import IcebergStateStore

    if silver_config.state_store == "__iceberg__":
        silver_config.state_store = IcebergStateStore(spark=spark)
    return silver_config


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def run(cfg: dict) -> dict:
    pipeline_type  = cfg.get("type", "").lower()
    execution_mode = cfg.get("mode", "batch").lower()   # batch | streaming

    if pipeline_type == "chain":
        return run_chain(cfg)

    if pipeline_type == "bronze" and execution_mode == "streaming":
        from killuhub.layers.streaming.pipeline import StreamingBronzePipeline
        config = _build_streaming_bronze(cfg)
        logger.info(
            "Dispatching StreamingBronzePipeline → %s [trigger=%s %s]",
            config.bronze_table,
            config.streaming_config.trigger,
            config.streaming_config.trigger_interval,
        )
        StreamingBronzePipeline(config).run()
        return {"status": "streaming_terminated"}

    if pipeline_type == "bronze":
        from killuhub import BronzePipeline
        config = _build_bronze(cfg)
        logger.info("Dispatching BronzePipeline → %s", config.bronze_table)
        return BronzePipeline(config).run()

    elif pipeline_type == "silver":
        from killuhub import SilverPipeline
        from killuhub.layers.silver.pipeline import SilverPipeline as SP
        config = _build_silver(cfg)

        # If IcebergStateStore was requested, we need to inject the spark session
        # after SilverPipeline starts its engine. We monkey-patch run() here
        # to intercept the engine start if needed.
        if config.state_store == "__iceberg__":
            engine_raw = cfg.get("engine", {})
            from killuhub.processing.spark_engine import SparkEngine
            engine = SparkEngine()
            engine.init_session(
                app_name="killuhub-state-init",
                warehouse=engine_raw.get("warehouse", "/tmp/killuhub-warehouse"),
                catalog_name=engine_raw.get("catalog_name", "local"),
            )
            config = _resolve_silver_state_store(config, engine.spark)
            # SilverPipeline will call getOrCreate() and reuse this session
            result = SilverPipeline(config).run()
            engine.stop()
            return result

        logger.info(
            "Dispatching SilverPipeline %s → %s",
            config.bronze_table, config.silver_table,
        )
        return SP(config).run()

    else:
        raise ValueError(
            f"Unknown pipeline type: '{pipeline_type}'. "
            "Expected 'bronze', 'silver', or 'chain'."
        )


def run_chain(cfg: dict) -> dict:
    """
    Run multiple pipeline stages in order from a single chain config.

    Each stage is an independent pipeline config (type: bronze|silver) merged
    with the top-level `engine:` block if the stage doesn't define its own.

    Auto-injection: if a silver stage omits `silver.bronze_table`, it is
    automatically filled in from the previous bronze stage's `bronze.table`.
    This means you only ever write the table name once.
    """
    stages = cfg.get("stages", [])
    if not stages:
        raise ValueError("Chain config must have at least one stage under 'stages'.")

    # Top-level engine block shared across stages (can be overridden per stage)
    shared_engine = cfg.get("engine", {})

    results = {}
    last_bronze_table: str | None = None

    for i, stage in enumerate(stages):
        name = stage.get("name", f"stage-{i + 1}")

        # Inherit shared engine if the stage doesn't define its own
        if "engine" not in stage and shared_engine:
            stage = {**stage, "engine": shared_engine}

        stage_type = stage.get("type", "").lower()

        # Auto-inject bronze_table into silver stage from the previous bronze stage
        if stage_type == "silver" and last_bronze_table:
            silver_section = stage.get("silver", {})
            if "bronze_table" not in silver_section:
                stage = {**stage, "silver": {**silver_section, "bronze_table": last_bronze_table}}
                logger.info(
                    "Chain — auto-injected bronze_table='%s' into '%s'",
                    last_bronze_table, name,
                )

        logger.info("Chain [%d/%d] — '%s' (type=%s)", i + 1, len(stages), name, stage_type)
        result = run(stage)
        results[name] = result
        logger.info("Chain [%d/%d] — '%s' finished: %s", i + 1, len(stages), name, result)

        # Track table name for the next stage
        if stage_type == "bronze":
            last_bronze_table = stage.get("bronze", {}).get("table")

    return {"status": "chain_completed", "stages": results}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="killuhub",
        description="KilluHub data ingestion framework",
    )
    parser.add_argument(
        "--config", "-c",
        default=os.environ.get("KILLUHUB_CONFIG"),
        help="Path to YAML pipeline config. Can also be set via KILLUHUB_CONFIG env var.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate config without running the pipeline.",
    )
    args = parser.parse_args()

    if not args.config:
        parser.error(
            "No config provided. Use --config <path> or set KILLUHUB_CONFIG."
        )

    logger.info("Loading config: %s", args.config)
    try:
        cfg = load_config(args.config)
    except (FileNotFoundError, ValueError, EnvironmentError) as exc:
        logger.error("Config error: %s", exc)
        sys.exit(1)

    if args.dry_run:
        if cfg.get("type") == "chain":
            stage_names = [s.get("name", f"stage-{i+1}") for i, s in enumerate(cfg.get("stages", []))]
            logger.info("Dry run — chain config with %d stages: %s", len(stage_names), stage_names)
        else:
            logger.info(
                "Dry run — config loaded successfully. type=%s mode=%s",
                cfg.get("type"), cfg.get("mode", "batch"),
            )
        logger.info("Config:\n%s", yaml.dump(cfg, default_flow_style=False))
        sys.exit(0)

    logger.info("Starting | type=%s", cfg.get("type"))
    try:
        summary = run(cfg)
        logger.info("Pipeline finished | %s", summary)
    except Exception:
        logger.exception("Pipeline failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
