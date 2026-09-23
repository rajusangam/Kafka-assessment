#!/usr/bin/env python3

import argparse
import json
import re
import socket
import subprocess
import sys


def run(command, timeout=30):
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", "command timed out"


def check_service(service):
    rc, _, stderr = run(["systemctl", "is-active", "--quiet", service])
    if rc != 0:
        return False, stderr.strip() or f"{service} is not active"
    return True, f"{service} is active"


def check_listener(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, port))
        return True, f"{host}:{port} is reachable"
    except OSError as exc:
        return False, str(exc)
    finally:
        sock.close()


def check_topics(kafka_topics, bootstrap_server):
    rc, stdout, stderr = run(
        [kafka_topics, "--bootstrap-server", bootstrap_server, "--describe"],
        timeout=60,
    )
    if rc != 0:
        return False, stderr.strip() or stdout.strip()
    return True, stdout


def detect_urp(topic_output):
    """Detect partitions where ISR count is smaller than replica count."""
    under_replicated = []

    for line in topic_output.splitlines():
        if "Replicas:" not in line or "Isr:" not in line:
            continue

        replicas_match = re.search(r"Replicas:\s*([0-9,\s]+)", line)
        isr_match = re.search(r"Isr:\s*([0-9,\s]+)", line)

        if not replicas_match or not isr_match:
            continue

        replicas = [
            value.strip()
            for value in replicas_match.group(1).split(",")
            if value.strip()
        ]
        isr = [
            value.strip()
            for value in isr_match.group(1).split(",")
            if value.strip()
        ]

        if len(isr) < len(replicas):
            under_replicated.append(
                {"replicas": replicas, "isr": isr}
            )

    return (False, under_replicated) if under_replicated else (True, [])


def main():
    parser = argparse.ArgumentParser(
        description="Kafka broker and ISR health check"
    )
    parser.add_argument("--service", default="kafka")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9092)
    parser.add_argument(
        "--kafka-topics",
        default="/opt/kafka/bin/kafka-topics.sh",
    )
    parser.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    args = parser.parse_args()

    results = []

    service_ok, service_message = check_service(args.service)
    results.append({
        "check": "kafka_service",
        "healthy": service_ok,
        "message": service_message,
    })

    listener_ok, listener_message = check_listener(args.host, args.port)
    results.append({
        "check": "kafka_listener",
        "healthy": listener_ok,
        "message": listener_message,
    })

    cli_ok, topic_output = check_topics(
        args.kafka_topics,
        args.bootstrap_server,
    )

    if not cli_ok:
        results.append({
            "check": "kafka_metadata",
            "healthy": False,
            "message": topic_output,
        })
    else:
        results.append({
            "check": "kafka_metadata",
            "healthy": True,
            "message": "Kafka metadata query succeeded",
        })

        urp_ok, urp = detect_urp(topic_output)
        results.append({
            "check": "under_replicated_partitions",
            "healthy": urp_ok,
            "count": len(urp),
            "partitions": urp,
        })

    healthy = all(item["healthy"] for item in results)

    print("Kafka Health Check")
    print("==================")
    for item in results:
        status = "PASS" if item["healthy"] else "FAIL"
        print(f"{item['check']:<30}: {status}")

    print()
    print(json.dumps(results, indent=2))
    print()
    print(f"RESULT: {'HEALTHY' if healthy else 'UNHEALTHY'}")

    sys.exit(0 if healthy else 1)


if __name__ == "__main__":
    main()
