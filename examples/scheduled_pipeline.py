"""
Example: scheduled pipeline with PipelineScheduler

Runs an hourly sync from a REST API to Iceberg,
and a 15-minute Postgres snapshot in parallel.

Run:
    python examples/scheduled_pipeline.py
"""
import logging

from killuhub import PipelineConfig, ConnectorConfig
from killuhub.ingestion import PipelineScheduler

logging.basicConfig(level=logging.INFO)

# --- REST API pipeline ---
api_config = PipelineConfig(
    connector_name="rest_api",
    connector_config=ConnectorConfig.from_dict(
        {
            "url": "https://api.example.com/transactions",
            "auth_type": "bearer",
            "auth_token": "MY_TOKEN",
            "data_key": "transactions",
            "pagination": "cursor",
            "page_size": 200,
        }
    ),
    engine_name="spark",
    storage_writer_name="iceberg",
    target_table="local.finance.transactions",
    write_mode="append",
    engine_kwargs={"warehouse": "/tmp/killuhub-warehouse"},
)

# --- Postgres pipeline ---
pg_config = PipelineConfig(
    connector_name="postgres",
    connector_config=ConnectorConfig.from_dict(
        {
            "host": "localhost",
            "database": "analytics",
            "user": "reader",
            "password": "pass",
            "query": "SELECT * FROM metrics WHERE updated_at > NOW() - INTERVAL '15 minutes'",
        }
    ),
    engine_name="spark",
    storage_writer_name="iceberg",
    target_table="local.analytics.metrics",
    write_mode="merge",
    storage_kwargs={"merge_keys": ["metric_id"]},
    engine_kwargs={"warehouse": "/tmp/killuhub-warehouse"},
)

if __name__ == "__main__":
    scheduler = PipelineScheduler()

    scheduler.add_cron_job(
        job_id="api_transactions_hourly",
        config=api_config,
        cron_expression="0 * * * *",   # every hour
    )

    scheduler.add_interval_job(
        job_id="postgres_metrics_15min",
        config=pg_config,
        minutes=15,
    )

    print("Scheduler running. Press Ctrl+C to stop.")
    scheduler.start(blocking=True)
