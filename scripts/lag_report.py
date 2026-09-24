#!/usr/bin/env python3
"""Report committed consumer offsets and their partition lag as JSON."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

try:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient
except ImportError as exc:  # Keep a useful message when the dependency is absent.
    print(
        "Error: confluent-kafka is required (install it with 'python -m pip install confluent-kafka').",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


RETRY_COUNT = 2
RETRY_DELAY_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 10.0


class GroupNotFound(Exception):
    """Raised when the requested group is not present in the cluster."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report committed offsets and lag for a consumer group.")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--api-key", required=True, help="Kafka API key")
    parser.add_argument("--api-secret", required=True, help="Kafka API secret")
    parser.add_argument("--group", required=True, help="Consumer group ID")
    return parser.parse_args()


def kafka_config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "bootstrap.servers": args.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": args.api_key,
        "sasl.password": args.api_secret,
        "socket.timeout.ms": int(REQUEST_TIMEOUT_SECONDS * 1000),
        "allow.auto.create.topics": False,
    }


def report_group(config: dict[str, Any], group_id: str) -> list[dict[str, int | str]]:
    admin = AdminClient(config)
    groups = admin.list_consumer_groups(request_timeout=REQUEST_TIMEOUT_SECONDS)
    if groups.errors:
        details = "; ".join(str(error) for error in groups.errors)
        raise RuntimeError(details)
    if not any(group.group_id == group_id for group in groups.valid):
        raise GroupNotFound(group_id)

    consumer_config = dict(config)
    consumer_config.update({"group.id": group_id, "enable.auto.commit": False})
    consumer = Consumer(consumer_config)
    try:
        metadata = consumer.list_topics(timeout=REQUEST_TIMEOUT_SECONDS)
        topic_partitions = [
            TopicPartition(topic_name, partition_id)
            for topic_name, topic in metadata.topics.items()
            if topic.error is None
            for partition_id in topic.partitions
        ]
        committed = consumer.committed(topic_partitions, timeout=REQUEST_TIMEOUT_SECONDS)
        rows: list[dict[str, int | str]] = []
        for partition in committed:
            current_offset = partition.offset
            if current_offset < 0:
                continue
            low, log_end_offset = consumer.get_watermark_offsets(
                partition, timeout=REQUEST_TIMEOUT_SECONDS, cached=False
            )
            rows.append(
                {
                    "topic": partition.topic,
                    "partition": partition.partition,
                    "current_offset": current_offset,
                    "log_end_offset": log_end_offset,
                    "lag": max(0, log_end_offset - current_offset),
                }
            )
        return sorted(rows, key=lambda row: (str(row["topic"]), int(row["partition"])))
    finally:
        consumer.close()


def main() -> int:
    args = parse_args()
    config = kafka_config(args)
    for attempt in range(RETRY_COUNT + 1):
        try:
            rows = report_group(config, args.group)
            print(json.dumps(rows, separators=(",", ":")))
            return 0
        except GroupNotFound:
            print(f"Error: consumer group '{args.group}' does not exist.", file=sys.stderr)
            return 2
        except (KafkaException, RuntimeError) as exc:
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            print(
                f"Error: unable to reach Kafka broker after {RETRY_COUNT} retries: {exc}",
                file=sys.stderr,
            )
            return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())