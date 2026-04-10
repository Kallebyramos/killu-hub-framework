"""
MySQL connector for KilluHub.

Uses mysql-connector-python with a server-side cursor (buffered=False) so large
tables are streamed row-by-row without loading the full result set into memory.

Required config keys:
    host, database, user, password, query

Optional:
    port       (int,  default 3306)
    batch_size (int,  default 1000) — rows fetched per round-trip
    ssl_ca     (str)  — path to CA certificate for TLS connections
    ssl_cert   (str)  — path to client certificate
    ssl_key    (str)  — path to client private key
"""
from typing import Any, Iterator

from killuhub.core.connector_interface import BaseConnector
from killuhub.core.config import ConnectorConfig


class MySQLConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._conn = None

    def connect(self) -> None:
        try:
            import mysql.connector
        except ImportError as e:
            raise ImportError(
                "mysql-connector-python is required for MySQLConnector. "
                "Install it with: pip install mysql-connector-python"
            ) from e

        kwargs: dict[str, Any] = {
            "host": self.config.require("host"),
            "port": self.config.get("port", 3306),
            "database": self.config.require("database"),
            "user": self.config.require("user"),
            "password": self.config.require("password"),
            # consume_results=True allows re-use of the connection after
            # the cursor is fully iterated without explicitly fetching all rows
            "consume_results": True,
        }

        ssl_ca = self.config.get("ssl_ca")
        if ssl_ca:
            kwargs["ssl_ca"] = ssl_ca
            kwargs["ssl_verify_cert"] = True
        ssl_cert = self.config.get("ssl_cert")
        if ssl_cert:
            kwargs["ssl_cert"] = ssl_cert
            kwargs["ssl_key"] = self.config.require("ssl_key")

        import mysql.connector
        self._conn = mysql.connector.connect(**kwargs)
        self._connected = True

    def extract(self) -> Iterator[dict[str, Any]]:
        if not self._connected:
            self.connect()

        query = self.config.require("query")
        batch_size = self.config.get("batch_size", 1_000)

        # buffered=False → server-side streaming cursor, safe for large tables.
        # fetchmany() pulls batch_size rows per round-trip to bound memory usage.
        cursor = self._conn.cursor(buffered=False, dictionary=True)
        try:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)
        finally:
            cursor.close()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self._connected = False
