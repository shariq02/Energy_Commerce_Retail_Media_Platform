"""Regenerate the contract JSON Schemas from the authored YAML.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: (1) check every src/schemas/**/*.yml parses; (2) regenerate
src/schemas/contracts/generated/*.schema.json from src/schemas/contracts/*.yml
via _generate_jsonschema.py; (3) check every generated file is valid JSON.
Run after editing any contract.

Usage:
    python3 scripts/utilities/build_schemas.py
    python3 scripts/utilities/build_schemas.py --check   # no writes; fail if stale
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_ROOT / "src" / "schemas"
GENERATED_DIR = SCHEMAS_DIR / "contracts" / "generated"
GENERATOR = SCHEMAS_DIR / "_generate_jsonschema.py"


def check_yaml() -> bool:
    print("==> YAML parse check (src/schemas/**/*.yml)")
    ok = True
    for path in sorted(SCHEMAS_DIR.rglob("*.yml")):
        rel = path.relative_to(REPO_ROOT)
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            print(f"  ERR {rel}: {exc}")
            ok = False
        else:
            print(f"  ok  {rel}")
    return ok


def run_generator(*, check: bool) -> bool:
    print(f"\n==> generating JSON Schemas{' (check only)' if check else ''}")
    cmd = [sys.executable, str(GENERATOR)]
    if check:
        cmd.append("--check")
    return subprocess.run(cmd, cwd=REPO_ROOT, check=False).returncode == 0


def check_json() -> bool:
    print("\n==> JSON validity check (src/schemas/contracts/generated/*.schema.json)")
    paths = sorted(GENERATED_DIR.glob("*.schema.json"))
    bad = 0
    for path in paths:
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"  ERR {path.name}: {exc}")
            bad += 1
    print(f"  {len(paths)} generated schema files, {len(paths) - bad} valid")
    return bad == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; fail if any generated schema is out of date",
    )
    args = parser.parse_args()

    if not check_yaml():
        return 1
    if not run_generator(check=args.check):
        return 1
    if not check_json():
        return 1
    print("\n==> done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
