"""
PostgreSQL connector for KilluHub.

Required config keys:
    host, port, database, user, password, query

Optional:
    batch_size (int, default 1000) — rows fetched per server-side cursor fetch
"""
from typing import Any, Iterator

from killuhub.core.connector_interface import BaseConnector
from killuhub.core.config import ConnectorConfig


class PostgresConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._conn = None
        self._cursor = None

    def connect(self) -> None:
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError(
                "psycopg2 is required for PostgresConnector. "
                "Install it with: pip install psycopg2-binary"
            ) from e

        self._conn = psycopg2.connect(
            host=self.config.require("host"),
            port=self.config.get("port", 5432),
            dbname=self.config.require("database"),
            user=self.config.require("user"),
            password=self.config.require("password"),
        )
        self._connected = True

    def extract(self) -> Iterator[dict[str, Any]]:
        if not self._connected:
            self.connect()

        query = self.config.require("query")
        batch_size = self.config.get("batch_size", 1_000)

        # Named cursor enables server-side pagination — safe for large tables
        with self._conn.cursor(name="killuhub_cursor") as cur:
            cur.itersize = batch_size
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            for row in cur:
                yield dict(zip(columns, row))

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self._connected = False
