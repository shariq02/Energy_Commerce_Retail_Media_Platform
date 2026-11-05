"""The Makefile commands that run the same on Windows, without make.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

Purpose: setup, test, lint and clean for a system without `make` (Windows). Each
command runs what the Makefile target of the same name runs. The gcloud and grafana
targets stay in the Makefile; they call their own programs.

Usage:
    python scripts/utilities/tasks.py <command>
Commands: setup, setup-dev, test, test-unit, test-integration, test-regression,
test-performance, lint, lint-fix, api-start, clean.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = [sys.executable, "-m"]
COVERAGE = [
    "--cov=src",
    "--cov=fastapi",
    "--cov=ai",
    "--cov=scripts/knowledge",
    "--cov-report=term-missing",
    "--cov-report=html",
]
# Debian marks its system Python as externally managed; Windows has no such mark.
SYSTEM_PACKAGES = [] if sys.platform == "win32" else ["--break-system-packages"]
COMMANDS: dict[str, list[list[str]]] = {
    "setup": [[*PYTHON, "pip", "install", *SYSTEM_PACKAGES, "-r", "requirements.txt"]],
    "setup-dev": [[*PYTHON, "pip", "install", *SYSTEM_PACKAGES, "-e", ".[dev]"]],
    "test": [[*PYTHON, "pytest", "tests/", "-v"]],
    "test-unit": [[*PYTHON, "pytest", "tests/unit/", "tests/ai/", "-v", *COVERAGE]],
    "test-integration": [[*PYTHON, "pytest", "tests/integration/", "-v"]],
    "test-regression": [[*PYTHON, "pytest", "tests/", "-m", "regression", "-v"]],
    "test-performance": [[*PYTHON, "pytest", "tests/", "-m", "performance", "-v"]],
    "lint": [
        [
            *PYTHON,
            "ruff",
            "check",
            ".",
            "--exclude",
            "data/",
            "--exclude",
            "docs/",
            "--exclude",
            ".terraform/",
        ]
    ],
    "lint-fix": [
        [*PYTHON, "ruff", "check", "--fix", "."],
        [*PYTHON, "ruff", "check", "--fix", "--unsafe-fixes", "."],
    ],
    "api-start": [
        [
            *PYTHON,
            "uvicorn",
            "fastapi.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--reload",
        ]
    ],
}


def clean() -> int:
    """Remove coverage output, the pytest cache and compiled Python files."""
    for name in (".coverage", "coverage.xml"):
        (REPO_ROOT / name).unlink(missing_ok=True)
    for name in ("htmlcov", ".pytest_cache"):
        shutil.rmtree(REPO_ROOT / name, ignore_errors=True)
    for folder in REPO_ROOT.rglob("__pycache__"):
        if ".git" not in folder.parts and ".terraform" not in folder.parts:
            shutil.rmtree(folder, ignore_errors=True)
    for pattern in ("*.pyc", "*.pyo"):
        for path in REPO_ROOT.rglob(pattern):
            if ".git" not in path.parts and ".terraform" not in path.parts:
                path.unlink(missing_ok=True)
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 1 or (argv[0] != "clean" and argv[0] not in COMMANDS):
        names = ", ".join([*COMMANDS, "clean"])
        print(f"usage: python scripts/utilities/tasks.py <command>\ncommands: {names}")
        return 2
    if argv[0] == "clean":
        return clean()
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    if sys.stdout.isatty():
        # pytest wraps its dot lines at this width
        env["COLUMNS"] = str(shutil.get_terminal_size().columns)
    for command in COMMANDS[argv[0]]:
        code = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False).returncode
        if code:
            return code
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
