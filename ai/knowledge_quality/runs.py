"""Stored records of evaluation runs and checks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

One JSON file per evaluation run (`<run id>.json`) and one for the latest check
(`latest_check.json`), in `ai/knowledge_quality/runs/<corpus>/`. The findings file
is made from these records. Library only.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from scripts.knowledge import _unit_common as uc

CHECK_NAME = "latest_check.json"


def runs_dir(ai_dir: Path, corpus_id: str) -> Path:
    return ai_dir / "knowledge_quality" / "runs" / corpus_id


def _write(path: Path, data: dict) -> None:
    uc.write_text(
        path, json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def save_run(ai_dir: Path, record: dict) -> Path:
    path = runs_dir(ai_dir, record["corpus_id"]) / f"{record['run_id']}.json"
    _write(path, record)
    return path


def load_run(ai_dir: Path, corpus_id: str, run_id: str) -> dict | None:
    path = runs_dir(ai_dir, corpus_id) / f"{run_id}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def latest_run(ai_dir: Path, corpus_id: str, split: str) -> dict | None:
    """The newest run of a split (the run id ends with a UTC time stamp)."""
    files = sorted(runs_dir(ai_dir, corpus_id).glob(f"{corpus_id}-{split}-*.json"))
    return json.loads(files[-1].read_text(encoding="utf-8")) if files else None


def latest_run_of_build(
    ai_dir: Path, corpus_id: str, split: str, build_id: str
) -> dict | None:
    """The newest run of a split that was made on the build."""
    files = sorted(runs_dir(ai_dir, corpus_id).glob(f"{corpus_id}-{split}-*.json"))
    for path in reversed(files):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record["build_id"] == build_id:
            return record
    return None


def save_check(ai_dir: Path, corpus_id: str, integrity, freshness) -> dict:
    record = {
        "created": datetime.now(UTC).isoformat(timespec="seconds"),
        "corpus_id": corpus_id,
        "integrity": {
            "problems": integrity.problems,
            "added": integrity.added,
            "removed": integrity.removed,
            "changed": integrity.changed,
            "notes": integrity.notes,
        },
        "freshness": {
            "checked": freshness.checked,
            "stale": freshness.stale,
            "unchecked": freshness.unchecked,
            "snapshot": freshness.snapshot,
        },
    }
    _write(runs_dir(ai_dir, corpus_id) / CHECK_NAME, record)
    return record


def load_check(ai_dir: Path, corpus_id: str) -> dict | None:
    path = runs_dir(ai_dir, corpus_id) / CHECK_NAME
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
