"""The labelled question set of a corpus: schema, approval, split.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A positive question names the unit that answers it; a negative question is one the
corpus cannot answer. Each question carries an approval with the hash of its
content, so a changed question is `stale` until it is approved again. Only approved
questions are evaluated. A question is a holdout question when the hash of its id
modulo 3 is 2, so the split does not move when questions are added. A question has a
cohort (default `base`) that is not part of its content hash. When the file names a
`holdout_cohort`, only the questions of that cohort can be holdout questions; the
others are tuning questions, because their holdout was read before. Library only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from scripts.knowledge import _unit_common as uc

CLASSES = (
    "identifier_lookup",
    "definitional_wording",
    "glossary_term",
    "cross_reference",
)
NEGATIVE = "negative"
TUNE = "tune"
HOLDOUT = "holdout"
_ID = re.compile(r"^q[0-9]{3,}$")


def questions_path(ai_dir: Path, corpus_id: str) -> Path:
    return ai_dir / "knowledge_quality" / "questions" / f"{corpus_id}.yml"


@dataclass
class Question:
    id: str
    text: str
    cls: str
    kind: str | None
    expected_unit_id: str | None
    approval: dict = field(default_factory=dict)
    cohort: str = "base"
    # Set by `load` when the file has a holdout cohort that this question is not in.
    tune_only: bool = False

    @property
    def negative(self) -> bool:
        return self.cls == NEGATIVE

    @property
    def content_hash(self) -> str:
        parts = [
            self.id,
            self.text,
            self.cls,
            self.kind or "",
            self.expected_unit_id or "",
        ]
        return uc.sha256_text("\t".join(parts))

    @property
    def status(self) -> str:
        """pending, approved, or stale (approved, but the question changed since)."""
        if self.approval.get("status") != "approved":
            return "pending"
        if self.approval.get("approved_hash") != self.content_hash:
            return "stale"
        return "approved"

    @property
    def split(self) -> str:
        if self.tune_only:
            return TUNE
        return HOLDOUT if int(uc.sha256_text(self.id)[:8], 16) % 3 == 2 else TUNE


@dataclass
class QuestionSet:
    corpus_id: str
    version: int
    questions: list[Question]
    holdout_cohort: str | None = None

    def of_cohort(self, cohort: str) -> list[Question]:
        return [q for q in self.approved() if q.cohort == cohort]

    def approved(self) -> list[Question]:
        return [q for q in self.questions if q.status == "approved"]

    def of_split(self, split: str) -> list[Question]:
        return [q for q in self.approved() if q.split == split]

    @property
    def holdout_hash(self) -> str:
        """Hash of the approved holdout questions only: a holdout read is once per build
        and per set of holdout questions."""
        rows = sorted(f"{q.id}\t{q.content_hash}" for q in self.of_split(HOLDOUT))
        return uc.sha256_text("\n".join(rows))

    @property
    def set_hash(self) -> str:
        rows = sorted(f"{q.id}\t{q.content_hash}" for q in self.approved())
        return uc.sha256_text("\n".join(rows))


def validate(question: Question, unit_ids: set[str] | None, kinds) -> None:
    where = question.id
    if not _ID.match(question.id) or not question.text.strip():
        raise ValueError(f"{where}: bad id or empty text")
    if question.negative:
        if question.kind or question.expected_unit_id:
            raise ValueError(f"{where}: a negative question names no unit or kind")
        return
    if question.cls not in CLASSES:
        raise ValueError(f"{where}: unknown class {question.cls!r}")
    if question.kind not in kinds or not question.expected_unit_id:
        raise ValueError(f"{where}: a positive question needs a kind and a unit id")
    if unit_ids is not None and question.expected_unit_id not in unit_ids:
        raise ValueError(f"{where}: unit {question.expected_unit_id} does not exist")


def load(path: Path, unit_ids: set[str] | None = None) -> QuestionSet:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    questions = [
        Question(
            id=str(row["id"]),
            text=str(row["text"]),
            cls=str(row["class"]),
            kind=row.get("kind"),
            expected_unit_id=row.get("expected_unit_id"),
            approval=row.get("approval") or {},
            cohort=str(row.get("cohort", "base")),
        )
        for row in data.get("questions", [])
    ]
    holdout_cohort = data.get("holdout_cohort")
    for question in questions:
        question.tune_only = bool(holdout_cohort) and question.cohort != holdout_cohort
    kinds = uc.KIND_DIRS if data["corpus"] == "curated" else ("wikipedia_article",)
    seen = set()
    for question in questions:
        if question.id in seen:
            raise ValueError(f"{question.id}: appears twice")
        seen.add(question.id)
        validate(question, unit_ids, kinds)
    return QuestionSet(data["corpus"], int(data["version"]), questions, holdout_cohort)


def save(path: Path, question_set: QuestionSet) -> None:
    rows = []
    for q in question_set.questions:
        row = {
            "id": q.id,
            "class": q.cls,
            "kind": q.kind,
            "expected_unit_id": q.expected_unit_id,
            "text": q.text,
            "approval": q.approval,
        }
        if q.cohort != "base":
            row["cohort"] = q.cohort
        rows.append(row)
    data = {
        "corpus": question_set.corpus_id,
        "version": question_set.version,
        "questions": rows,
    }
    if question_set.holdout_cohort:
        data["holdout_cohort"] = question_set.holdout_cohort
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
    uc.write_text(path, text)


def approve(
    question_set: QuestionSet, ids: list[str], approver: str, today: str
) -> list[Question]:
    """Approve the chosen questions that wait for approval; the set version rises."""
    known = {q.id for q in question_set.questions}
    unknown = sorted(set(ids) - known)
    if unknown:
        raise ValueError(f"unknown question id(s): {unknown}")
    chosen = [
        q for q in question_set.questions if q.id in ids and q.status != "approved"
    ]
    for question in chosen:
        question.approval = {
            "status": "approved",
            "approver": approver,
            "approved_at": today,
            "approved_hash": question.content_hash,
        }
    if chosen:
        question_set.version += 1
    return chosen
