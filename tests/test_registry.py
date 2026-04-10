import pytest

from killuhub.core.registry import Registry
from killuhub.core.config import ConnectorConfig
from killuhub.core.connector_interface import BaseConnector
from killuhub.core.exceptions import ConnectorNotFoundError, EngineNotFoundError


class _DummyConnector(BaseConnector):
    def connect(self): pass
    def extract(self): yield {"id": 1}
    def close(self): pass


def test_register_and_retrieve_connector():
    reg = Registry()
    reg.register_connector("dummy", _DummyConnector)
    cls = reg.get_connector("dummy")
    assert cls is _DummyConnector


def test_connector_not_found_raises():
    reg = Registry()
    with pytest.raises(ConnectorNotFoundError):
        reg.get_connector("nonexistent")


def test_list_connectors():
    reg = Registry()
    reg.register_connector("a", _DummyConnector)
    reg.register_connector("b", _DummyConnector)
    assert set(reg.list_connectors()) == {"a", "b"}


def test_default_registry_has_builtin_connectors():
    from killuhub.core.registry import default_registry
    import killuhub.connectors  # trigger registration

    names = default_registry.list_connectors()
    assert "postgres" in names
    assert "kafka" in names
    assert "s3" in names
    assert "rest_api" in names


def test_default_registry_has_builtin_engines():
    from killuhub.core.registry import default_registry
    import killuhub.processing  # trigger registration

    names = default_registry.list_engines()
    assert "spark" in names
    assert "flink" in names
