# Connectors — Study Guide

A connector is the entry point of any pipeline. Its only job is to connect to a data source and yield records as plain Python dicts. It knows nothing about Spark, Iceberg, or any other layer.

---

## Table of contents

1. [The connector contract](#1-the-connector-contract)
2. [Why plain dicts?](#2-why-plain-dicts)
3. [PostgreSQL connector](#3-postgresql-connector)
4. [MySQL connector](#4-mysql-connector)
5. [Kafka connector](#5-kafka-connector)
6. [REST API connector](#6-rest-api-connector)
7. [How registration works](#7-how-registration-works)
8. [Writing your own connector](#8-writing-your-own-connector)

---

## 1. The connector contract

Every connector must implement three methods:

```python
def connect(self) -> None:
    """Open a connection to the source."""

def extract(self) -> Iterator[dict[str, Any]]:
    """Yield records one at a time."""

def close(self) -> None:
    """Release all resources."""
```

The base class provides `__enter__` / `__exit__` so all connectors work with Python's `with` statement:

```python
with PostgresConnector(config) as conn:
    for record in conn.extract():
        ...
# close() called automatically here
```

The `with` statement guarantees `close()` runs even if an exception occurs — no resource leaks.

---

## 2. Why plain dicts?

Records are `dict[str, Any]` — the most universal Python data structure. This keeps connectors completely decoupled from the processing layer:

- The connector doesn't know about Spark schemas
- The engine receives a list of dicts and creates its own DataFrame
- If you switch from Spark to Flink, the connector doesn't change at all

```python
# Connector yields this (universal)
{"order_id": 42, "amount": 99.90, "customer": "Alice"}

# SparkEngine turns it into this
spark.createDataFrame([{"order_id": 42, ...}])

# FlinkEngine turns it into this
t_env.from_pandas(pd.DataFrame([{"order_id": 42, ...}]))
```

---

## 3. PostgreSQL connector

**File:** [killuhub/connectors/postgres/connector.py](../../killuhub/connectors/postgres/connector.py)

### The server-side cursor

The most important concept in this connector is the **named (server-side) cursor**. Here's the problem it solves:

A naive approach loads all rows into the client's memory:
```python
# BAD: fetches all 50M rows at once
cursor.execute("SELECT * FROM events")
rows = cursor.fetchall()  # 50M rows in RAM → OOM crash
```

A server-side cursor keeps the result set on the database server and streams rows in pages:
```python
# GOOD: server holds the data, client fetches batch_size rows at a time
with conn.cursor(name="killuhub_cursor") as cur:
    cur.itersize = 1_000  # fetch 1000 rows per network round-trip
    cur.execute(query)
    for row in cur:        # streams from server, doesn't load all at once
        yield row
```

In psycopg2, a cursor with a `name` argument is a server-side cursor. `itersize` controls how many rows are fetched per round-trip to the server.

### Config keys

| Key           | Required | Default | Description                        |
|---------------|----------|---------|------------------------------------|
| `host`        | Yes      | —       | Database host                      |
| `port`        | No       | `5432`  | Database port                      |
| `database`    | Yes      | —       | Database name                      |
| `user`        | Yes      | —       | Username                           |
| `password`    | Yes      | —       | Password                           |
| `query`       | Yes      | —       | SQL query to execute               |
| `batch_size`  | No       | `1000`  | Rows per server-side cursor fetch  |

### Example

```python
ConnectorConfig.from_dict({
    "host": "prod-db.internal",
    "database": "warehouse",
    "user": "readonly",
    "password": "secret",
    "query": """
        SELECT order_id, customer_id, total, created_at
        FROM orders
        WHERE created_at >= '2024-01-01'
    """,
    "batch_size": 5000,
})
```

### How column names are captured

```python
cur.execute(query)
columns = [desc[0] for desc in cur.description]  # ["order_id", "customer_id", ...]
for row in cur:
    yield dict(zip(columns, row))  # {"order_id": 1, "customer_id": 99, ...}
```

`cur.description` is a list of `Column` objects; `desc[0]` is the column name. `zip(columns, row)` pairs each name with its value.

---

## 4. MySQL connector

**File:** [killuhub/connectors/mysql/connector.py](../../killuhub/connectors/mysql/connector.py)

The MySQL connector uses a **server-side streaming cursor** (`buffered=False`) so large result sets are never loaded entirely into memory. Records are fetched in pages via `fetchmany(batch_size)`.

### Incremental extraction

Like the Postgres connector, MySQL uses a watermark column in the SQL query to filter only new or updated rows:

```sql
SELECT * FROM orders WHERE updated_at > '2024-01-01T00:00:00'
```

The watermark state is saved after each successful run and passed into the next query automatically.

### Optional TLS

If `ssl_ca` is set in the config, the connector enables TLS with certificate verification:

```yaml
connector:
  name: mysql
  config:
    host: ${MYSQL_HOST}
    database: shop
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    query: "SELECT * FROM orders"
    ssl_ca: /etc/ssl/certs/mysql-ca.pem   # optional TLS
```

### Config keys

| Key          | Required | Default  | Description                           |
|--------------|----------|----------|---------------------------------------|
| `host`       | Yes      | —        | Database host                         |
| `port`       | No       | `3306`   | Database port                         |
| `database`   | Yes      | —        | Database name                         |
| `user`       | Yes      | —        | Username                              |
| `password`   | Yes      | —        | Password                              |
| `query`      | Yes      | —        | SQL query to execute                  |
| `batch_size` | No       | `1000`   | Rows per `fetchmany` call             |
| `ssl_ca`     | No       | —        | Path to CA certificate for TLS        |
| `ssl_cert`   | No       | —        | Path to client certificate            |
| `ssl_key`    | No       | —        | Path to client private key            |

---

## 5. Kafka connector

**File:** [killuhub/connectors/kafka/connector.py](../../killuhub/connectors/kafka/connector.py)

### Kafka fundamentals

Kafka is a distributed event streaming platform. Data is organized into **topics** (think of them as append-only logs). Producers write events; consumers read them.

A **consumer group** (`group_id`) allows multiple consumers to share the work of reading a topic — each partition is assigned to one consumer in the group. Kafka tracks which offset (position) each group has read up to.

### Batch vs streaming

KilluHub's Kafka connector is designed for **batch mode**: it reads up to `max_records` events and then stops. This is different from a streaming consumer that runs forever.

This design fits data pipelines that run on a schedule (e.g., every 15 minutes): consume whatever arrived since the last run, process it, commit the offsets.

### Manual commit

The connector uses `enable.auto.commit: false` and commits manually after each record is yielded:

```python
yield record
self._consumer.commit(message=msg)
```

Why? Auto-commit marks records as read on a timer, regardless of whether your pipeline successfully processed them. Manual commit means: "I have processed this record and written it to Iceberg — now mark it as consumed." This prevents data loss if the pipeline crashes mid-batch.

### End-of-stream detection

```python
if msg is None:
    # poll() returned nothing within the timeout → no new messages → stop
    break

if msg.error().code() == KafkaError._PARTITION_EOF:
    # reached the end of a partition → stop
    break
```

`poll(timeout)` returns `None` if no message arrives within `timeout` seconds. For batch pipelines this is the "done" signal.

### Config keys

| Key                   | Required | Default      | Description                              |
|-----------------------|----------|--------------|------------------------------------------|
| `bootstrap_servers`   | Yes      | —            | Broker list, e.g. `"host:9092"`          |
| `topic`               | Yes      | —            | Topic to consume                         |
| `group_id`            | Yes      | —            | Consumer group identifier                |
| `auto_offset_reset`   | No       | `"earliest"` | Where to start: `"earliest"` or `"latest"` |
| `max_records`         | No       | None         | Stop after N records (batch cap)         |
| `poll_timeout_ms`     | No       | `1000`       | Max wait per poll call (ms)              |
| `value_deserializer`  | No       | `"json"`     | `"json"` or `"string"`                  |

---

## 6. REST API connector

S3 `ListObjectsV2` returns at most 1000 objects per call. The connector uses a **paginator** to handle buckets with thousands of files:

**File:** [killuhub/connectors/rest_api/connector.py](../../killuhub/connectors/rest_api/connector.py)

This is the most complex connector because REST APIs have no standard for pagination. KilluHub supports three strategies.

### Pagination strategies

#### `"none"` — single request
```
GET /api/data
→ returns all records in one response
```

#### `"page"` — page number
```
GET /api/orders?page=1&page_size=100
GET /api/orders?page=2&page_size=100
GET /api/orders?page=3&page_size=100  → empty response → stop
```

The connector increments the page number until the API returns an empty list.

#### `"cursor"` — cursor-based
```
GET /api/events
→ {"data": [...], "next_cursor": "abc123"}

GET /api/events?cursor=abc123
→ {"data": [...], "next_cursor": "def456"}

GET /api/events?cursor=def456
→ {"data": [...], "next_cursor": null}  → stop
```

Cursor pagination is more reliable than page numbers for large or frequently-updated datasets. The API controls exactly which records come next.

#### `"offset"` — numeric offset
```
GET /api/items?offset=0&limit=100
GET /api/items?offset=100&limit=100
GET /api/items?offset=200&limit=100  → fewer than 100 results → stop
```

### Authentication

```python
# Bearer token
ConnectorConfig.from_dict({
    "url": "https://api.example.com/data",
    "auth_type": "bearer",
    "auth_token": "eyJhbG...",
})

# Basic auth
ConnectorConfig.from_dict({
    "url": "https://api.example.com/data",
    "auth_type": "basic",
    "auth_user": "admin",
    "auth_password": "secret",
})
```

### Extracting records from nested responses

APIs often nest the actual records under a key like `"data"` or `"results"`:

```json
{
    "status": "ok",
    "total": 1500,
    "results": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
}
```

Set `data_key: "results"` and the connector extracts just that list.

### Config keys

| Key              | Required | Default        | Description                          |
|------------------|----------|----------------|--------------------------------------|
| `url`            | Yes      | —              | API endpoint URL                     |
| `method`         | No       | `"GET"`        | HTTP method                          |
| `auth_type`      | No       | —              | `"bearer"` / `"basic"` / none        |
| `auth_token`     | No       | —              | Bearer token                         |
| `data_key`       | No       | —              | JSON key containing the records list |
| `pagination`     | No       | `"none"`       | `"none"` / `"page"` / `"cursor"` / `"offset"` |
| `page_size`      | No       | `100`          | Records per request                  |
| `max_pages`      | No       | —              | Safety cap to prevent infinite loops |
| `cursor_key`     | No       | `"next_cursor"`| JSON key holding the next cursor     |

---

## 7. How registration works

All built-in connectors self-register when their package is imported:

```python
# killuhub/connectors/__init__.py
from killuhub.core.registry import default_registry

default_registry.register_connector("postgres", PostgresConnector)
default_registry.register_connector("mysql", MySQLConnector)
default_registry.register_connector("kafka", KafkaConnector)
default_registry.register_connector("rest_api", RestApiConnector)
```

This runs as a side-effect of `import killuhub.connectors`, which happens automatically when you `import killuhub`.

The Pipeline then resolves the class at runtime:

```python
connector_cls = default_registry.get_connector("postgres")  # returns PostgresConnector class
connector = connector_cls(config)                            # instantiates it
```

---

## 8. Writing your own connector

Any external system can be made into a KilluHub connector in ~20 lines:

```python
from typing import Any, Iterator
from killuhub.core.connector_interface import BaseConnector
from killuhub.core.config import ConnectorConfig
from killuhub.core.registry import default_registry


class ElasticsearchConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._client = None

    def connect(self) -> None:
        from elasticsearch import Elasticsearch
        self._client = Elasticsearch(self.config.require("hosts"))
        self._connected = True

    def extract(self) -> Iterator[dict[str, Any]]:
        index = self.config.require("index")
        query = self.config.get("query", {"match_all": {}})

        # Elasticsearch scroll API for large result sets
        resp = self._client.search(
            index=index,
            body={"query": query},
            scroll="2m",
            size=self.config.get("batch_size", 1000),
        )
        scroll_id = resp["_scroll_id"]

        while True:
            hits = resp["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                yield {"_id": hit["_id"], **hit["_source"]}
            resp = self._client.scroll(scroll_id=scroll_id, scroll="2m")

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._connected = False


# Register once at startup
default_registry.register_connector("elasticsearch", ElasticsearchConnector)
```

Now any pipeline can use `connector_name="elasticsearch"`.

---

## Summary

| Connector    | Transport           | Key feature                                        |
|--------------|---------------------|----------------------------------------------------|
| `postgres`   | TCP/psycopg2        | Server-side cursor — safe for 100M+ rows           |
| `mysql`      | TCP/mysql-connector | Streaming cursor with `fetchmany`, optional TLS    |
| `kafka`      | Kafka protocol      | Batch consumption with manual offset commit        |
| `rest_api`   | HTTPS/requests      | 4 pagination strategies, bearer + basic auth       |

> **S3 is a destination, not a source.** Iceberg tables are stored on S3 via the engine's `warehouse` path. There is no S3 source connector — use `postgres`, `mysql`, `kafka`, or `rest_api` to ingest data into your Bronze Iceberg tables.
