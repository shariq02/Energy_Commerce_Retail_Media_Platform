"""Accepted state of a corpus: which build is designated, and the holdout reads.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

One file per corpus. A designation is an event with its own hash and the hash of
the event before it; the file is written only by `designate`, and `verify` finds a
hand edit. The designated event names the build, the collection, the corpus
version and fingerprint, the evaluation run, the retrieval config, the checks and
a snapshot of the units. `previous` is the event it replaced. The holdout reads
are recorded here too. Library only.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.knowledge import _unit_common as uc


def state_path(ai_dir: Path, corpus_id: str) -> Path:
    return ai_dir / "knowledge_quality" / "accepted" / f"{corpus_id}.json"


def empty(corpus_id: str) -> dict:
    return {
        "corpus_id": corpus_id,
        "designated": None,
        "previous": None,
        "history": [],
        "holdout_reads": [],
    }


def read(ai_dir: Path, corpus_id: str) -> dict:
    path = state_path(ai_dir, corpus_id)
    if not path.is_file():
        return empty(corpus_id)
    return json.loads(path.read_text(encoding="utf-8"))


def write(ai_dir: Path, state: dict) -> None:
    text = json.dumps(state, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    uc.write_text(state_path(ai_dir, state["corpus_id"]), text)


def event_hash(event: dict) -> str:
    body = {key: value for key, value in event.items() if key != "event_hash"}
    return uc.sha256_text(json.dumps(body, sort_keys=True, ensure_ascii=False))


def designate(ai_dir: Path, corpus_id: str, body: dict) -> dict:
    """Append a designation event; the designated one becomes `previous`."""
    state = read(ai_dir, corpus_id)
    last = state["history"][-1] if state["history"] else None
    event = {**body, "previous_hash": last["event_hash"] if last else None}
    event["event_hash"] = event_hash(event)
    state["history"].append(event)
    state["previous"] = state["designated"]
    state["designated"] = event
    write(ai_dir, state)
    return event


def designated_event(ai_dir: Path, corpus_id: str) -> dict | None:
    return read(ai_dir, corpus_id)["designated"]


def keep_collections(ai_dir: Path, corpus_id: str) -> set[str]:
    """Collections that must not be pruned: the designated and the previous one."""
    state = read(ai_dir, corpus_id)
    events = (state["designated"], state["previous"])
    return {event["collection"] for event in events if event}


def holdout_read(ai_dir: Path, corpus_id: str, build_id: str) -> dict | None:
    for entry in read(ai_dir, corpus_id)["holdout_reads"]:
        if entry["build_id"] == build_id:
            return entry
    return None


def record_holdout_read(ai_dir: Path, corpus_id: str, entry: dict) -> None:
    state = read(ai_dir, corpus_id)
    state["holdout_reads"].append(entry)
    write(ai_dir, state)


def verify(ai_dir: Path, corpus_id: str) -> list[str]:
    """Problems in the chain: a changed event, a broken link, a wrong pointer."""
    state = read(ai_dir, corpus_id)
    history = state["history"]
    problems = []
    before = None
    for number, event in enumerate(history, 1):
        if event.get("event_hash") != event_hash(event):
            problems.append(f"designation event {number} was changed by hand")
        if event.get("previous_hash") != before:
            problems.append(f"designation event {number} does not follow event before")
        before = event.get("event_hash")
    if state["designated"] != (history[-1] if history else None):
        problems.append("the designated pointer is not the last designation event")
    if state["previous"] != (history[-2] if len(history) > 1 else None):
        problems.append("the previous pointer is not the designation event before")
    return problems
