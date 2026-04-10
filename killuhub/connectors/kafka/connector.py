"""
Kafka connector for KilluHub.

Required config keys:
    bootstrap_servers  — comma-separated broker list
    topic              — topic to consume
    group_id           — consumer group id

Optional:
    auto_offset_reset  (default "earliest")
    max_records        (int)  — stop after N records (useful for batch jobs)
    poll_timeout_ms    (int, default 1000)
    value_deserializer ("json" | "string", default "json")
"""
import json
from typing import Any, Iterator

from killuhub.core.connector_interface import BaseConnector
from killuhub.core.config import ConnectorConfig


class KafkaConnector(BaseConnector):
    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self._consumer = None

    def connect(self) -> None:
        try:
            from confluent_kafka import Consumer
        except ImportError as e:
            raise ImportError(
                "confluent-kafka is required for KafkaConnector. "
                "Install it with: pip install confluent-kafka"
            ) from e

        from confluent_kafka import Consumer

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.config.require("bootstrap_servers"),
                "group.id": self.config.require("group_id"),
                "auto.offset.reset": self.config.get("auto_offset_reset", "earliest"),
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self.config.require("topic")])
        self._connected = True

    def extract(self) -> Iterator[dict[str, Any]]:
        if not self._connected:
            self.connect()

        max_records = self.config.get("max_records", None)
        poll_timeout = self.config.get("poll_timeout_ms", 1_000) / 1_000
        deserializer = self.config.get("value_deserializer", "json")
        count = 0

        while True:
            msg = self._consumer.poll(timeout=poll_timeout)

            if msg is None:
                # No message within timeout — treat as end of stream for batch mode
                break

            if msg.error():
                from confluent_kafka import KafkaError
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise RuntimeError(f"Kafka error: {msg.error()}")

            raw = msg.value()
            if deserializer == "json":
                record = json.loads(raw.decode("utf-8"))
            else:
                record = {"value": raw.decode("utf-8")}

            yield record
            self._consumer.commit(message=msg)
            count += 1

            if max_records and count >= max_records:
                break

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            self._connected = False
