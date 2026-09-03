"""CDC pipeline -- Redpanda topic management.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: create (and, on request, delete) the one-per-table CDC topics on the
local Redpanda broker via rpk. CDC topics only -- no historical replay topics.
"""

from __future__ import annotations

import argparse
import subprocess

from src.ingestion.cdc import config

_PARTITIONS = "1"
_RETENTION_MS = str(7 * 24 * 60 * 60 * 1000)  # 7 days is plenty for a demo


def _rpk(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["rpk", *args, "--brokers", config.BOOTSTRAP_SERVERS],
        capture_output=True,
        text=True,
        check=False,
    )


def list_topics() -> set[str]:
    result = _rpk("topic", "list")
    names = set()
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if parts:
            names.add(parts[0])
    return names


def create(*, recreate: bool = False) -> int:
    existing = list_topics()
    for topic in config.all_topics():
        if topic in existing:
            if not recreate:
                print(f"OK  exists: {topic}")
                continue
            _rpk("topic", "delete", topic)
        result = _rpk(
            "topic",
            "create",
            topic,
            "--partitions",
            _PARTITIONS,
            "--config",
            f"retention.ms={_RETENTION_MS}",
        )
        if result.returncode != 0:
            print(f"FAIL  create {topic}: {result.stderr.strip()}")
            return 1
        print(f"OK  created: {topic}")
    return 0


def delete() -> int:
    for topic in config.all_topics():
        _rpk("topic", "delete", topic)
        print(f"deleted (if present): {topic}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage CDC topics on Redpanda")
    parser.add_argument("action", choices=["create", "recreate", "delete"])
    args = parser.parse_args()
    if args.action == "delete":
        return delete()
    return create(recreate=args.action == "recreate")


if __name__ == "__main__":
    raise SystemExit(main())
