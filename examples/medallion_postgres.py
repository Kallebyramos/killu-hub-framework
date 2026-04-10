"""
Example: Full medallion pipeline — Postgres → Bronze → Silver

Stage 1 — Bronze (FULL):
  Reads all orders from Postgres.
  Appends raw records to Iceberg with metadata columns.

Stage 2 — Silver (INCREMENTAL):
  Reads only records added to Bronze since the last Silver run.
  Parses dates, casts types, deduplicates, adds date dimensions.
  Appends enriched records to the Silver Iceberg table.

Run:
    python examples/medallion_postgres.py
"""
import logging

from killuhub import (
    BronzePipeline, BronzeConfig,
    SilverPipeline, SilverConfig,
    Pipeline, PipelineConfig, ConnectorConfig,
    BatchMode, BatchConfig,
)

logging.basicConfig(level=logging.INFO)

ENGINE_KWARGS = {"warehouse": "/tmp/killuhub-warehouse", "catalog_name": "local"}

# ---------------------------------------------------------------------------
# Stage 1 — Bronze ingestion (FULL)
# ---------------------------------------------------------------------------

bronze_config = BronzeConfig(
    pipeline_config=PipelineConfig(
        connector_name="postgres",
        connector_config=ConnectorConfig.from_dict({
            "host": "localhost",
            "database": "shop",
            "user": "admin",
            "password": "secret",
            "query": "SELECT * FROM orders",
            "batch_size": 10_000,
        }),
        engine_name="spark",
        engine_kwargs=ENGINE_KWARGS,
    ),
    batch_config=BatchConfig(mode=BatchMode.FULL),
    bronze_table="local.bronze.orders",
    source_name="postgres.shop.orders",
)

# ---------------------------------------------------------------------------
# Stage 2 — Silver transformation (INCREMENTAL)
# ---------------------------------------------------------------------------

def custom_filter(df):
    """Only keep orders with a positive amount — business rule."""
    return df.filter("amount > 0")


silver_config = SilverConfig(
    bronze_table="local.bronze.orders",
    silver_table="local.silver.orders",
    batch_config=BatchConfig(
        mode=BatchMode.INCREMENTAL,
        watermark_column="_ingested_at",  # filter on Bronze metadata column
    ),
    # Standard transforms
    key_columns=["order_id"],             # deduplicate by order_id
    date_columns=["created_at", "updated_at"],  # parse strings → timestamps
    date_dimension_column="created_at",   # add year/month/day/hour/date columns
    type_map={
        "amount":   "double",
        "quantity": "int",
        "status":   "string",
    },
    null_check_columns=["order_id", "customer_id"],  # drop rows with null keys
    partition_by=["order_date"],          # partition Silver by derived date
    # User transform applied after standard transforms
    user_transform_fn=custom_filter,
    engine_kwargs=ENGINE_KWARGS,
    storage_kwargs={"catalog_name": "local"},
)

# ---------------------------------------------------------------------------
# Run both stages
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Stage 1: Bronze ingestion ===")
    bronze_summary = BronzePipeline(bronze_config).run()
    print(bronze_summary)

    print("\n=== Stage 2: Silver transformation ===")
    silver_summary = SilverPipeline(silver_config).run()
    print(silver_summary)
