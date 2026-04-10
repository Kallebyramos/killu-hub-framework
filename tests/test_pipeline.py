"""
Unit tests for the Pipeline class.

We mock the connector, engine, and writer so no real infrastructure
is needed to run these tests.
"""
import pytest
from unittest.mock import MagicMock, patch

from killuhub.core.config import PipelineConfig, ConnectorConfig
from killuhub.core.registry import Registry
from killuhub.ingestion.pipeline import Pipeline
from killuhub.core.exceptions import PipelineError


# --- Fixtures ---

def _make_config(**overrides):
    base = dict(
        connector_name="mock_connector",
        connector_config=ConnectorConfig.from_dict({}),
        engine_name="mock_engine",
        storage_writer_name="iceberg",
        target_table="local.test.table",
        write_mode="append",
        batch_size=2,
    )
    base.update(overrides)
    return PipelineConfig(**base)


class _FakeConnector:
    def __init__(self, config): pass
    def connect(self): pass
    def extract(self):
        yield {"id": 1, "value": "a"}
        yield {"id": 2, "value": "b"}
        yield {"id": 3, "value": "c"}
    def close(self): pass
    def __enter__(self): self.connect(); return self
    def __exit__(self, *_): self.close()


class _FakeEngine:
    spark = MagicMock()

    def init_session(self, app_name, **kwargs): pass
    def create_dataframe(self, records): return MagicMock(name="df")
    def apply_transform(self, df, fn): return df if fn is None else fn(df)
    def stop(self): pass


# --- Tests ---

def test_pipeline_runs_and_returns_summary():
    config = _make_config()

    with (
        patch("killuhub.ingestion.pipeline.default_registry") as mock_reg,
        patch("killuhub.ingestion.pipeline.IcebergWriter") as mock_writer_cls,
    ):
        mock_reg.get_connector.return_value = _FakeConnector
        mock_reg.get_engine.return_value = _FakeEngine

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        summary = Pipeline(config).run()

    # 3 records with batch_size=2 → 2 batches
    assert summary["total_records"] == 3
    assert summary["total_batches"] == 2
    assert summary["target_table"] == "local.test.table"


def test_pipeline_applies_transform():
    transform_called = []

    def my_transform(df):
        transform_called.append(True)
        return df

    config = _make_config(transform_fn=my_transform)

    with (
        patch("killuhub.ingestion.pipeline.default_registry") as mock_reg,
        patch("killuhub.ingestion.pipeline.IcebergWriter"),
    ):
        mock_reg.get_connector.return_value = _FakeConnector
        mock_reg.get_engine.return_value = _FakeEngine
        Pipeline(config).run()

    assert len(transform_called) == 2  # one call per batch


def test_pipeline_wraps_connector_error():
    class _BrokenConnector(_FakeConnector):
        def connect(self):
            raise RuntimeError("connection refused")

    config = _make_config()

    with (
        patch("killuhub.ingestion.pipeline.default_registry") as mock_reg,
        patch("killuhub.ingestion.pipeline.IcebergWriter"),
    ):
        mock_reg.get_connector.return_value = _BrokenConnector
        mock_reg.get_engine.return_value = _FakeEngine

        with pytest.raises(PipelineError):
            Pipeline(config).run()
