# Core Layer — Study Guide

The `core/` package is the skeleton of KilluHub. Nothing in this layer depends on Spark, Kafka, or any external library. It defines **what** each component must do, without saying **how**. Every other layer depends on `core/` — `core/` depends on nothing.

---

## Table of contents

1. [Why a core layer?](#1-why-a-core-layer)
2. [Abstract Base Classes (ABCs)](#2-abstract-base-classes-abcs)
3. [connector_interface.py — BaseConnector](#3-connector_interfacepy--baseconnector)
4. [engine_interface.py — BaseEngine](#4-engine_interfacepy--baseengine)
5. [storage_interface.py — BaseStorageWriter](#5-storage_interfacepy--basestoragewriter)
6. [config.py — ConnectorConfig and PipelineConfig](#6-configpy--connectorconfig-and-pipelineconfig)
7. [registry.py — The Registry pattern](#7-registrypy--the-registry-pattern)
8. [exceptions.py — Exception hierarchy](#8-exceptionspy--exception-hierarchy)
9. [How everything connects](#9-how-everything-connects)

---

## 1. Why a core layer?

Software becomes hard to change when components know too much about each other. If `Pipeline` directly imported `PostgresConnector`, you couldn't swap it for `KafkaConnector` without modifying `Pipeline`. If `IcebergWriter` was hardcoded into `Pipeline`, you couldn't test the pipeline without a real Iceberg catalog.

The solution is **Dependency Inversion**: high-level modules (Pipeline) depend on abstractions, not on concrete implementations. The core layer provides those abstractions.

```
Pipeline  →  BaseConnector  ←  PostgresConnector
          →  BaseEngine     ←  SparkEngine
          →  BaseStorageWriter ← IcebergWriter
```

`Pipeline` only ever talks to the interfaces. The actual implementations can be anything.

---

## 2. Abstract Base Classes (ABCs)

Python's `abc` module provides the `ABC` class and the `@abstractmethod` decorator. A class that inherits from `ABC` and has at least one `@abstractmethod` **cannot be instantiated directly** — you must subclass it and implement all abstract methods.

```python
from abc import ABC, abstractmethod

class Animal(ABC):
    @abstractmethod
    def speak(self) -> str:
        """Every animal must implement this."""

# This would raise TypeError: Can't instantiate abstract class
# a = Animal()

class Dog(Animal):
    def speak(self) -> str:
        return "Woof"

d = Dog()  # This works
```

ABCs act as a contract: any class that passes `isinstance(obj, BaseConnector)` is guaranteed to have `connect()`, `extract()`, and `close()` methods. The Pipeline can rely on that contract without knowing the implementation.

---

## 3. connector_interface.py — BaseConnector

```python
class BaseConnector(ABC):
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def extract(self) -> Iterator[dict[str, Any]]: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()
```

### Key design decisions

**`extract()` returns an Iterator, not a list**

This is critical for large data sources. If `extract()` returned `list[dict]`, you'd load millions of rows into memory before processing anything. By returning an `Iterator` (via `yield`), records are pulled one at a time — the Pipeline then groups them into batches with `islice`.

```python
# Bad: loads everything into RAM first
def extract(self) -> list[dict]:
    return list(cursor.fetchall())  # 10M rows = crash

# Good: yields one record at a time
def extract(self) -> Iterator[dict]:
    for row in cursor:
        yield dict(zip(columns, row))
```

**Context manager support (`__enter__` / `__exit__`)**

Implemented in the base class so all connectors automatically support `with` syntax. The subclass only needs to implement `connect()` and `close()` — the base class wires them into the context manager protocol.

```python
with PostgresConnector(config) as connector:
    for record in connector.extract():
        process(record)
# close() is called automatically, even if an exception occurs
```

**`_connected` flag**

A simple guard that subclasses use to prevent double-connection and to check state in `extract()`.

---

## 4. engine_interface.py — BaseEngine

```python
class BaseEngine(ABC):
    @abstractmethod
    def init_session(self, app_name: str, **kwargs) -> None: ...

    @abstractmethod
    def create_dataframe(self, records: list[dict[str, Any]]) -> Any: ...

    @abstractmethod
    def apply_transform(self, df: Any, transform_fn) -> Any: ...

    @abstractmethod
    def stop(self) -> None: ...
```

### Key design decisions

**`create_dataframe()` takes `list[dict]`**

The engine's job is to convert raw Python dicts into its native data structure. For Spark that's a `DataFrame`, for Flink that's a `Table`. By accepting plain dicts, the interface is completely engine-agnostic — the connector doesn't know about DataFrames at all.

**`apply_transform()` takes a callable**

Transformations are user-defined functions. The engine's job is just to apply them. This means:
- Users write transforms in native Spark/Flink code (full power of the engine)
- The Pipeline doesn't need to know what the transform does
- If `transform_fn` is `None`, the engine returns the DataFrame unchanged

```python
# User's transform — pure Spark code
def my_transform(df):
    from pyspark.sql import functions as F
    return (
        df.filter("amount > 0")
          .withColumn("year", F.year("created_at"))
          .dropDuplicates(["order_id"])
    )

config = PipelineConfig(..., transform_fn=my_transform)
```

**Why `init_session()` instead of `__init__()`?**

Session creation is expensive (starts JVM for Spark, connects to Flink cluster). Separating `__init__` from `init_session()` allows the Registry to instantiate the engine class without starting anything, and lets the Pipeline control exactly when the session starts.

---

## 5. storage_interface.py — BaseStorageWriter

```python
class BaseStorageWriter(ABC):
    @abstractmethod
    def write(self, df: Any, table_name: str, mode: str = "append") -> None: ...

    @abstractmethod
    def table_exists(self, table_name: str) -> bool: ...
```

### Write modes

All writers must support three modes:

| Mode        | Behavior                                             | Use case                          |
|-------------|------------------------------------------------------|-----------------------------------|
| `append`    | Add new rows, never touch existing ones              | Event logs, audit trails          |
| `overwrite` | Replace all data in the table (or partition)         | Full snapshot loads               |
| `merge`     | Insert if new, update if exists (based on key match) | Slowly Changing Dimensions, CDC   |

The `merge` mode is the most complex — it requires unique keys (`merge_keys`) and uses `MERGE INTO` SQL under the hood.

---

## 6. config.py — ConnectorConfig and PipelineConfig

### ConnectorConfig

A thin wrapper around a plain dict. Instead of having every connector define its own config class, we use a generic key-value bag:

```python
config = ConnectorConfig.from_dict({
    "host": "localhost",
    "port": 5432,
    "database": "shop",
    "user": "admin",
    "password": "secret",
    "query": "SELECT * FROM orders",
})

# .get() — returns default if missing
port = config.get("port", 5432)

# .require() — raises ConnectorConfigError if missing
host = config.require("host")
```

The `require()` method gives clear error messages when a connector is misconfigured, instead of a cryptic `KeyError`.

### PipelineConfig

A dataclass that describes a complete pipeline run. All fields have defaults except the connector ones:

```python
@dataclass
class PipelineConfig:
    connector_name: str          # required — "postgres", "kafka", etc.
    connector_config: ConnectorConfig  # required
    engine_name: str = "spark"
    storage_writer_name: str = "iceberg"
    target_table: str = ""
    write_mode: str = "append"
    batch_size: int = 10_000
    transform_fn: Any = None     # optional Spark/Flink transform
    engine_kwargs: dict = field(default_factory=dict)
    storage_kwargs: dict = field(default_factory=dict)
```

`engine_kwargs` and `storage_kwargs` are escape hatches for engine/writer-specific options (warehouse path, catalog name, partition columns, merge keys) without polluting the shared interface.

---

## 7. registry.py — The Registry pattern

The Registry is the mechanism that makes KilluHub pluggable. It's a dictionary that maps string names to classes.

```python
class Registry:
    def __init__(self):
        self._connectors: dict[str, Type[BaseConnector]] = {}
        self._engines:    dict[str, Type[BaseEngine]] = {}
        self._writers:    dict[str, Type[BaseStorageWriter]] = {}

    def register_connector(self, name: str, cls: Type[BaseConnector]) -> None:
        self._connectors[name] = cls

    def get_connector(self, name: str) -> Type[BaseConnector]:
        if name not in self._connectors:
            raise ConnectorNotFoundError(name)
        return self._connectors[name]
```

### The singleton

```python
# registry.py
default_registry = Registry()
```

This single instance is imported by everyone. All built-in connectors register themselves on import:

```python
# connectors/__init__.py
from killuhub.core.registry import default_registry
default_registry.register_connector("postgres", PostgresConnector)
default_registry.register_connector("kafka", KafkaConnector)
```

When `killuhub/__init__.py` runs `import killuhub.connectors`, the registration side-effect happens automatically.

### Adding a custom connector

```python
from killuhub.core import BaseConnector, default_registry

class MongoDBConnector(BaseConnector):
    def connect(self): ...
    def extract(self): ...
    def close(self): ...

# Register it — now usable anywhere by name
default_registry.register_connector("mongodb", MongoDBConnector)
```

### Why the Registry pattern?

The alternative is a big `if/elif` in the Pipeline:

```python
# Bad: every new connector requires changing pipeline.py
if connector_name == "postgres":
    connector = PostgresConnector(config)
elif connector_name == "kafka":
    connector = KafkaConnector(config)
elif connector_name == "mongodb":  # must edit this file for every new connector
    ...
```

With the Registry, `Pipeline` never needs to change. It just calls `registry.get_connector(name)` and gets back the right class, regardless of how many connectors exist.

This is the **Open/Closed Principle**: open for extension (add new connectors), closed for modification (don't change Pipeline).

---

## 8. exceptions.py — Exception hierarchy

```
Exception
└── KilluHubError              (base — catch this to handle any KilluHub error)
    ├── ConnectorNotFoundError  (unknown connector name in registry)
    ├── EngineNotFoundError     (unknown engine name in registry)
    ├── StorageWriterNotFoundError
    ├── ConnectorConfigError    (required config key missing)
    └── PipelineError           (runtime failure during pipeline execution)
```

Having a base `KilluHubError` lets callers catch all framework errors in one place:

```python
try:
    Pipeline(config).run()
except KilluHubError as e:
    logger.error("KilluHub failed: %s", e)
    # send alert, retry, etc.
```

---

## 9. How everything connects

Here's the full dependency graph of the core layer:

```
PipelineConfig
    ├── connector_name ──▶ Registry.get_connector() ──▶ BaseConnector subclass
    ├── engine_name    ──▶ Registry.get_engine()    ──▶ BaseEngine subclass
    └── storage_writer ──▶ _WRITER_CLASSES dict     ──▶ BaseStorageWriter subclass

ConnectorConfig
    └── used by every BaseConnector subclass via self.config.require() / .get()
```

The Pipeline never imports `PostgresConnector`, `SparkEngine`, or `IcebergWriter` directly. It only knows about the abstract interfaces and the Registry. This is what makes the framework truly pluggable.

---

## Summary

| File                     | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `connector_interface.py` | Contract for all data sources — connect, extract, close  |
| `engine_interface.py`    | Contract for processing engines — session, df, transform  |
| `storage_interface.py`   | Contract for storage writers — write, table_exists        |
| `config.py`              | Type-safe configuration objects for connectors + pipeline |
| `registry.py`            | Central lookup map — names to classes, pluggable design   |
| `exceptions.py`          | Clean error hierarchy for catching framework errors       |
