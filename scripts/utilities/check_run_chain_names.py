"""Undefined names in Databricks notebooks, checked across their `%run` chain.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

Purpose: ruff leaves F821 (undefined name) off for the notebooks that take names
from `%run` libraries. This check joins each notebook with its `%run` chain and
runs ruff F821 on the joined text, so a name that neither the notebook nor its
libraries define fails before a run. A library (`_*.py`) takes some names from
the calling notebook, so it is checked only through the notebooks that run it.

Usage:
    python scripts/utilities/check_run_chain_names.py [path ...]
With repo-relative paths, the notebooks among them and the notebooks that run one
of them are checked; without, every notebook under databricks/.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_ROOT = REPO_ROOT / "databricks"
RUN_LINE = re.compile(r"^\s*#\s*MAGIC\s+%run\s+(\S+)", re.MULTILINE)
# A stdin file name outside the per-file F821 exemptions of pyproject.toml.
STDIN_NAME = "run_chain_check.py"


def run_targets(path: Path) -> list[Path]:
    """The existing files that the `%run` lines of `path` name, in order (a
    missing target is a failure of the CI `%run` path step)."""
    targets = []
    for target in RUN_LINE.findall(path.read_text(encoding="utf-8")):
        resolved = (path.parent / target).resolve()
        if resolved.suffix != ".py":
            resolved = resolved.with_name(resolved.name + ".py")
        if resolved.is_file():
            targets.append(resolved)
    return targets


def run_chain(path: Path, seen: set | None = None) -> list[Path]:
    """The libraries that `path` runs, depth first, each once, in run order."""
    seen = set() if seen is None else seen
    chain = []
    for target in run_targets(path):
        if target in seen:
            continue
        seen.add(target)
        chain += run_chain(target, seen)
        chain.append(target)
    return chain


def joined_source(path: Path) -> tuple[str, int]:
    """The chain's libraries and the notebook as one text, and the number of lines
    before the notebook."""
    prefix = "".join(
        library.read_text(encoding="utf-8").rstrip("\n") + "\n"
        for library in run_chain(path)
    )
    return prefix + path.read_text(encoding="utf-8"), prefix.count("\n")


def ruff_command() -> list[str]:
    ruff = shutil.which("ruff")
    return [ruff] if ruff else [sys.executable, "-m", "ruff"]


def undefined_names(path: Path) -> list[str]:
    """`<notebook>:<line>: <message>` for each undefined name in the notebook's own
    lines."""
    text, offset = joined_source(path)
    command = [
        *ruff_command(),
        "check",
        "--select",
        "F821",
        "--output-format",
        "json",
        "--stdin-filename",
        STDIN_NAME,
        "-",
    ]
    done = subprocess.run(
        command,
        input=text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT,
        check=False,
    )
    if done.returncode not in (0, 1):
        message = f"ruff failed on {path}: {done.stderr.strip()}"
        raise RuntimeError(message)
    name = path.relative_to(REPO_ROOT).as_posix()
    return [
        f"{name}:{item['location']['row'] - offset}: {item['message']}"
        for item in json.loads(done.stdout or "[]")
        if item["location"]["row"] > offset
    ]


def notebooks(changed: list[str] | None = None) -> list[Path]:
    """The notebooks (not `_*.py` libraries) with a `%run` line: all, or those in
    `changed` and those that run a changed file."""
    wanted = None
    if changed is not None:
        wanted = {(REPO_ROOT / name).resolve() for name in changed}
    found = []
    for path in sorted(NOTEBOOK_ROOT.rglob("*.py")):
        if path.name.startswith("_"):
            continue
        if not RUN_LINE.search(path.read_text(encoding="utf-8")):
            continue
        resolved = path.resolve()
        if wanted is None or resolved in wanted or wanted & set(run_chain(resolved)):
            found.append(path)
    return found


def main(argv: list[str] | None = None) -> int:
    paths = sys.argv[1:] if argv is None else argv
    checked = notebooks(paths or None)
    problems = [line for path in checked for line in undefined_names(path)]
    print(f"==> checked {len(checked)} notebook(s) and their %run chains (F821)")
    if problems:
        print(f"==> found {len(problems)} undefined name(s):")
        print("\n".join(f"  {line}" for line in problems))
        return 1
    print("==> none found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
