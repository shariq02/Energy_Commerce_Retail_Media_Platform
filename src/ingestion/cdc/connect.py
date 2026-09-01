"""CDC pipeline -- native Kafka Connect (standalone) control.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: start and stop a standalone Kafka Connect worker running the Debezium
PostgreSQL connector, using the worker and connector properties under cdc/. No
container runtime -- the Connect scripts from a local Kafka install are invoked
directly. Readiness is checked against the worker's REST endpoint.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from pathlib import Path

import requests

from src.ingestion.cdc import config

WORKER_PROPERTIES = (
    config.REPO_ROOT / "cdc" / "config" / "connect-standalone.properties"
)
CONNECTOR_PROPERTIES = (
    config.REPO_ROOT / "cdc" / "debezium" / "ecrmap-postgres.properties"
)
LOG_FILE = config.REPO_ROOT / "logs" / "ingestion" / "kafka-connect.log"
PID_FILE = config.CONNECT_DIR / "connect.pid"
REST_URL = os.getenv("KAFKA_CONNECT_REST_URL", "http://localhost:8083")

_CANDIDATE_HOMES = (
    os.getenv("KAFKA_HOME", ""),
    str(Path.home() / "kafka"),
    "/opt/kafka",
    "/usr/local/kafka",
)


def _connect_script() -> Path:
    for home in _CANDIDATE_HOMES:
        if not home:
            continue
        candidate = Path(home) / "bin" / "connect-standalone.sh"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "connect-standalone.sh not found -- set KAFKA_HOME to a local Kafka install "
        "(scripts/setup/install_kafka_connect.sh downloads one)"
    )


def status() -> dict | None:
    try:
        resp = requests.get(
            f"{REST_URL}/connectors/{config.CONNECTOR_NAME}/status", timeout=5
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    return resp.json()


def start(wait: float = 90.0) -> int:
    for path in (WORKER_PROPERTIES, CONNECTOR_PROPERTIES):
        if not path.exists():
            print(f"FAIL  missing config: {path}")
            return 1
    script = _connect_script()
    config.ensure_dirs()
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    if PID_FILE.exists():
        print(f"connect already started (pid file {PID_FILE}); stop it first")
        return 1

    with LOG_FILE.open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(
            [str(script), str(WORKER_PROPERTIES), str(CONNECTOR_PROPERTIES)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    print(f"kafka connect starting (pid {proc.pid}), log -> {LOG_FILE}")

    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            print(
                f"FAIL  connect exited early (code {proc.returncode}); see {LOG_FILE}"
            )
            PID_FILE.unlink(missing_ok=True)
            return 1
        state = status()
        if state is not None:
            conn_state = state.get("connector", {}).get("state")
            print(f"OK  connector {config.CONNECTOR_NAME} state={conn_state}")
            return 0 if conn_state in ("RUNNING", "PAUSED") else 1
        time.sleep(3)
    print(f"FAIL  connector did not report RUNNING within {wait:.0f}s; see {LOG_FILE}")
    return 1


def stop() -> int:
    if not PID_FILE.exists():
        print("no pid file -- connect not started by this tool")
        return 0
    pid = int(PID_FILE.read_text(encoding="utf-8"))
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        print(f"sent SIGTERM to connect process group {pid}")
    except ProcessLookupError:
        print(f"process {pid} not running")
    PID_FILE.unlink(missing_ok=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Control native Kafka Connect")
    parser.add_argument("action", choices=["start", "stop", "status"])
    args = parser.parse_args()
    if args.action == "start":
        return start()
    if args.action == "stop":
        return stop()
    state = status()
    print(state if state else "connector not reachable")
    return 0 if state else 1


if __name__ == "__main__":
    raise SystemExit(main())
