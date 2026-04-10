"""
Example: PostgreSQL → Spark → Iceberg

Reads an orders table from Postgres, filters invalid rows with a
Spark transform, and writes to an Iceberg table partitioned by date.

Run:
    python examples/postgres_to_iceberg.py
"""
from killuhub import Pipeline, PipelineConfig, ConnectorConfig


def transform(df):
    """Keep only orders with a positive amount."""
    return df.filter("amount > 0")


config = PipelineConfig(
    connector_name="postgres",
    connector_config=ConnectorConfig.from_dict(
        {
            "host": "localhost",
            "port": 5432,
            "database": "shop",
            "user": "admin",
            "password": "secret",
            "query": "SELECT id, customer_id, amount, created_at FROM orders",
            "batch_size": 5_000,
        }
    ),
    engine_name="spark",
    storage_writer_name="iceberg",
    target_table="local.shop.orders",
    write_mode="append",
    batch_size=5_000,
    transform_fn=transform,
    engine_kwargs={
        "catalog_name": "local",
        "warehouse": "/tmp/killuhub-warehouse",
    },
    storage_kwargs={
        "catalog_name": "local",
        "partition_by": ["created_at"],
    },
)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    summary = Pipeline(config).run()
    print(summary)
