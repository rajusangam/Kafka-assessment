#!/usr/bin/env python3
"""
Simple Kafka cluster / topic health check for Confluent Cloud.

Usage:
    export KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-central1.gcp.confluent.cloud:9092"
    export KAFKA_API_KEY="..."
    export KAFKA_API_SECRET="..."
    python3 kafka_health_check.py --topic orders.v1
"""

import argparse
import os
import sys

try:
    from confluent_kafka.admin import AdminClient
except ImportError:
    print("confluent-kafka is required: pip install confluent-kafka", file=sys.stderr)
    sys.exit(1)


def build_admin_client() -> AdminClient:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    api_key = os.environ.get("KAFKA_API_KEY")
    api_secret = os.environ.get("KAFKA_API_SECRET")

    if not all([bootstrap, api_key, api_secret]):
        print(
            "KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET "
            "must all be set.",
            file=sys.stderr,
        )
        sys.exit(1)

    return AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        }
    )


def check_topic(admin: AdminClient, topic_name: str) -> bool:
    metadata = admin.list_topics(timeout=10)

    if topic_name not in metadata.topics:
        print(f"FAIL: topic '{topic_name}' not found on the cluster.")
        return False

    topic_metadata = metadata.topics[topic_name]
    if topic_metadata.error is not None:
        print(f"FAIL: topic '{topic_name}' has error: {topic_metadata.error}")
        return False

    partitions = topic_metadata.partitions
    print(f"OK: topic '{topic_name}' found with {len(partitions)} partition(s).")

    for pid, pmeta in partitions.items():
        replicas = pmeta.replicas
        isrs = pmeta.isrs
        status = "OK" if len(isrs) >= 2 else "WARN"
        print(
            f"  partition {pid}: leader={pmeta.leader} "
            f"replicas={replicas} isrs={isrs} [{status}]"
        )

    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka cluster/topic health check")
    parser.add_argument(
        "--topic",
        default="orders.v1",
        help="Topic to check (default: orders.v1)",
    )
    args = parser.parse_args()

    admin = build_admin_client()

    try:
        cluster_metadata = admin.list_topics(timeout=10)
        print(f"Connected. Cluster has {len(cluster_metadata.topics)} topic(s).")
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: could not reach cluster: {exc}", file=sys.stderr)
        sys.exit(1)

    ok = check_topic(admin, args.topic)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
