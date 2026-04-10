from killuhub.connectors.postgres.connector import PostgresConnector
from killuhub.connectors.mysql.connector import MySQLConnector
from killuhub.connectors.kafka.connector import KafkaConnector
from killuhub.connectors.rest_api.connector import RestApiConnector
from killuhub.core.registry import default_registry

# Auto-register all built-in connectors
default_registry.register_connector("postgres", PostgresConnector)
default_registry.register_connector("mysql", MySQLConnector)
default_registry.register_connector("kafka", KafkaConnector)
default_registry.register_connector("rest_api", RestApiConnector)

__all__ = [
    "PostgresConnector",
    "MySQLConnector",
    "KafkaConnector",
    "RestApiConnector",
]
