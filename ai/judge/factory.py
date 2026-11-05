"""Make the judge of a build, and store its fitted parameters.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The parameters of the local judge are one JSON file in the build folder of the corpus
build they were fitted on; a file made for another build is refused. A judge is made
only when a switch of the retrieval config is on. Library only.
"""

from __future__ import annotations

import json
from pathlib import Path

from ai.embeddings import build_record as br
from ai.judge.local import LocalJudge
from scripts.knowledge import _unit_common as uc

PARAMS_FILE = "judge.json"


def params_path(ai_dir: Path, corpus_id: str, build_id: str) -> Path:
    return br.build_dir(ai_dir, corpus_id, build_id) / PARAMS_FILE


def save_params(ai_dir: Path, corpus_id: str, build_id: str, params: dict) -> Path:
    path = params_path(ai_dir, corpus_id, build_id)
    uc.write_text(path, json.dumps(params, indent=2, sort_keys=True) + "\n")
    return path


def load_params(ai_dir: Path, corpus_id: str, build_id: str) -> dict | None:
    path = params_path(ai_dir, corpus_id, build_id)
    if not path.is_file():
        return None
    params = json.loads(path.read_text(encoding="utf-8"))
    if params.get("build_id") != build_id:
        raise ValueError(f"{path} was fitted on another build")
    return params


def switches_on(config) -> bool:
    return bool(config.judge_filter or config.judge_gate or config.judge_relevance)


def make_judge(config, ai_dir: Path, corpus_id: str, build_id: str, embed=None):
    """The local judge, or None (no switch on, or no fitted parameters)."""
    if not switches_on(config):
        return None
    params = load_params(ai_dir, corpus_id, build_id)
    return LocalJudge(params, embed) if params else None
