"""Lint + format gate.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: run pre-commit (ruff-check + ruff-format) across the repo. If both hooks
pass, nothing else happens. If either fails, auto-fix with ruff, verify with the
check-only commands, then re-run pre-commit to confirm a clean tree.

Usage:
    python3 scripts/utilities/lint.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXCLUDES = ["--exclude", "data/", "--exclude", "docs/", "--exclude", ".terraform/"]


def run(cmd: list[str]) -> int:
    print(f"==> {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=REPO_ROOT, check=False).returncode


def main() -> int:
    if run(["pre-commit", "run", "--all-files"]) == 0:
        print("\n==> pre-commit passed. Nothing to fix.")
        return 0

    print("\n==> pre-commit reported issues. Auto-fixing with ruff.")
    run(["ruff", "check", ".", *EXCLUDES, "--fix"])
    run(["ruff", "format", ".", *EXCLUDES])

    print("\n==> verify (check only)")
    check_rc = run(["ruff", "check", ".", *EXCLUDES])
    fmt_rc = run(["ruff", "format", ".", *EXCLUDES, "--check"])

    print("\n==> re-running pre-commit")
    pc_rc = run(["pre-commit", "run", "--all-files"])

    if check_rc == fmt_rc == pc_rc == 0:
        print("\n==> clean after auto-fix. Review and stage the changes.")
        return 0
    print(
        "\n==> still failing after auto-fix. Manual changes needed (see output above)."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
