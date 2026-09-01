"""CDC pipeline -- ad-hoc end-to-end cycle runner.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: drive one full local CDC cycle by hand -- ensure topics, register the
event schemas, take the pre-change baseline, apply operational changes, consume
what Debezium produced into the landing files, then upload to the Databricks
Volume. Scheduling this is a later concern; this script just sequences the steps
so a change can be watched flowing through.
"""

from __future__ import annotations

import argparse
import subprocess
import sys

from src.ingestion.cdc import config

_STEPS = ("topics", "schemas", "baseline", "changes", "consume", "sync")


def _run(module: str, *args: str) -> int:
    print(f"\n--- {module} {' '.join(args)} ---")
    return subprocess.run(
        [sys.executable, "-m", module, *args], cwd=config.REPO_ROOT, check=False
    ).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one local CDC cycle")
    parser.add_argument("--cycles", type=int, default=40, help="change-driver cycles")
    parser.add_argument("--idle-timeout", type=float, default=20.0)
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        choices=_STEPS,
        help="steps to skip (e.g. --skip sync when Databricks creds are absent)",
    )
    args = parser.parse_args()
    config.ensure_dirs()

    plan: list[tuple[str, list[str]]] = [
        ("src.ingestion.cdc.topics", ["create"]),
        ("src.ingestion.cdc.register_schemas", []),
        ("src.ingestion.cdc.change_driver", ["--snapshot"]),
        ("src.ingestion.cdc.change_driver", [f"--cycles={args.cycles}"]),
        ("src.ingestion.cdc.consumer", [f"--idle-timeout={args.idle_timeout}"]),
        ("src.ingestion.cdc.sync_to_databricks", []),
    ]

    for step_name, (module, mod_args) in zip(_STEPS, plan, strict=True):
        if step_name in args.skip:
            print(f"\n--- {step_name}: skipped ---")
            continue
        rc = _run(module, *mod_args)
        if rc != 0:
            print(f"\nFAIL  step '{step_name}' returned {rc}; stopping cycle")
            return rc

    print("\n" + "=" * 70)
    print("CDC CYCLE COMPLETE")
    print(f"landing dir : {config.LANDING_DIR}")
    print(f"volume root : {config.volume_root()}")
    print("next        : run the Databricks CDC Bronze + reconciliation notebooks")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
