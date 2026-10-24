"""Quality checks of the knowledge corpus: the one command of the quality work.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Without a command the whole flow runs (the command `all`). Commands:
  all              the whole flow in order: check, approve questions, evaluate the
                   tuning questions, thresholds, evaluate the holdout, accept; it
                   stops at the first failure, at a threshold conflict and while
                   questions wait for approval
  check            integrity of the corpus and its indexes, and freshness of the units
  approve-questions  approve evaluation questions (--id, --all-pending)
  evaluate         evaluate every retrieval type: --split tune, or --split holdout
                   (the holdout is read once per candidate and the read is recorded)
  thresholds       set the score thresholds from the tuning questions; writes a new
                   retrieval config version
  accept           apply the acceptance rule to the holdout run of the candidate and,
                   when it passes and the owner approves, designate the build
  status           what is designated, the holdout reads and the config versions
Every command that writes (also each step of `all`) prints its result first and asks before writing (y or
yes); --apply writes without asking, --dry-run never writes. The collection and the
indexes are only read.

Usage (repository root):
    python -m ai.knowledge_quality.run [--corpus curated|wikipedia]
    python -m ai.knowledge_quality.run evaluate --split tune
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from ai.embeddings import embedder as emb
from ai.embeddings import progress
from ai.knowledge_quality import (
    acceptance,
    evaluation,
    findings,
    freshness,
    runs,
    state,
    thresholds,
)
from ai.knowledge_quality import integrity as iq
from ai.knowledge_quality import questions as qs
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from scripts.knowledge import _unit_common as uc

CORPORA = ("curated", "wikipedia")
COMMANDS = (
    "all",
    "check",
    "approve-questions",
    "evaluate",
    "thresholds",
    "accept",
    "status",
)


def writing(args: argparse.Namespace) -> bool:
    """True when the owner wants the result written."""
    if args.apply:
        return True
    return False if args.dry_run else uc.confirm_apply()


def today() -> str:
    return datetime.now(UTC).date().isoformat()


def unit_ids(args: argparse.Namespace) -> set[str] | None:
    if args.corpus != "curated":
        return None
    manifest = st.read_manifest(args.corpus, args.corpus_dir)
    return {unit["unit_id"] for unit in manifest["units"]}


def load_questions(args: argparse.Namespace) -> qs.QuestionSet:
    path = qs.questions_path(args.ai_dir, args.corpus)
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found: there is no question set yet")
    return qs.load(path, unit_ids(args))


def finish(args: argparse.Namespace) -> None:
    path = findings.export(args.ai_dir, args.root, args.corpus)
    print(f"Findings written: {path.relative_to(args.root).as_posix()}")


def print_metrics(run: dict) -> None:
    for row in findings.metric_rows(run["metrics"], None):
        print("  " + " | ".join(str(cell) for cell in row[1:]))


def cmd_check(args: argparse.Namespace) -> int:
    report = iq.check(args.corpus, args.corpus_dir, args.model_dir, progress.show)
    fresh = freshness.check(args.corpus, args.corpus_dir)
    print(f"Integrity: {'PASS' if report.ok else 'FAIL'}")
    for problem in report.problems:
        print(f"  FAIL {problem}")
    for label, items in (
        ("added", report.added),
        ("removed", report.removed),
        ("changed", report.changed),
    ):
        print(f"  units {label} since the designation: {len(items)} {items[:5]}")
    for note in report.notes:
        print(f"  note: {note}")
    print(
        f"Freshness: {fresh.checked} source(s) checked, {len(fresh.stale)} stale, "
        f"{len(fresh.unchecked)} unchecked"
    )
    for item in fresh.stale:
        print(f"  STALE {item['unit_id']}: {item['source']} changed")
    if fresh.snapshot:
        print(f"  snapshot: {fresh.snapshot}")
    if writing(args):
        runs.save_check(args.ai_dir, args.corpus, report, fresh)
        finish(args)
    else:
        print(uc.NOT_WRITTEN)
    return 0 if report.ok else 1


def cmd_approve_questions(args: argparse.Namespace) -> int:
    question_set = load_questions(args)
    waiting = [q for q in question_set.questions if q.status != "approved"]
    print(
        f"{len(waiting)} of {len(question_set.questions)} question(s) wait for approval"
    )
    for q in waiting:
        print(f"  {q.id} [{q.cls}] {q.expected_unit_id or 'no unit'}: {q.text}")
    ids = [q.id for q in waiting] if args.all_pending else list(args.id or [])
    if not ids:
        print("Choose questions with --id or --all-pending.")
        return 0
    approver = (args.approver or "").strip()
    if not approver and uc.is_interactive():
        approver = uc.ask("Approver name: ")
    if not approver:
        print("No name given, so no approval.")
        return 1
    if not writing(args):
        print(uc.NOT_WRITTEN)
        return 0
    chosen = qs.approve(question_set, ids, approver, today())
    qs.save(qs.questions_path(args.ai_dir, args.corpus), question_set)
    print(
        f"Approved {len(chosen)} question(s) as {approver}; set version {question_set.version}."
    )
    return 0


def candidate(args: argparse.Namespace) -> tuple[str, st.Store]:
    record = st.read_record(args.corpus, args.corpus_dir)
    build_id = record["build_id"]
    return build_id, st.load(args.corpus, args.corpus_dir, args.model_dir, build_id)


def cmd_evaluate(args: argparse.Namespace) -> int:
    question_set = load_questions(args)
    chosen = question_set.of_split(args.split)
    if not chosen:
        print(f"There is no approved {args.split} question.")
        return 1
    build_id, store = candidate(args)
    if args.split == qs.HOLDOUT:
        if args.dry_run:
            print("The holdout read is always recorded, so it cannot be a dry run.")
            return 1
        if state.holdout_read(args.ai_dir, args.corpus, build_id):
            print(
                f"The holdout was already read for build {build_id}; once per candidate."
            )
            return 1
        print(f"{len(chosen)} holdout question(s) will be read for build {build_id}.")
        print("The read is recorded and cannot be repeated for this candidate.")
        if not writing(args):
            print("Nothing read.")
            return 0
    number, config = thresholds.latest(args.ai_dir, args.corpus)
    if args.config_version is not None:
        number = args.config_version
        config = RetrievalConfig.from_dict(
            thresholds.read_version(args.ai_dir, args.corpus, number)["config"]
        )
    outcomes = evaluation.evaluate(store, chosen, config, progress.show)
    record = evaluation.run_record(
        store, args.corpus, args.split, outcomes, question_set, config, number
    )
    print(
        f"Run {record['run_id']}: {len(outcomes)} question(s), config version {number}"
    )
    print_metrics(record)
    if args.split == qs.HOLDOUT or writing(args):
        runs.save_run(args.ai_dir, record)
        if args.split == qs.HOLDOUT:
            state.record_holdout_read(
                args.ai_dir,
                args.corpus,
                {
                    "build_id": build_id,
                    "run_id": record["run_id"],
                    "question_set_hash": question_set.set_hash,
                    "read_on": today(),
                    "questions": len(outcomes),
                },
            )
        finish(args)
    else:
        print(uc.NOT_WRITTEN)
    return 0


def cmd_thresholds(args: argparse.Namespace) -> int:
    question_set = load_questions(args)
    chosen = question_set.of_split(qs.TUNE)
    if not chosen:
        print("There is no approved tuning question.")
        return 1
    _, store = candidate(args)
    _, latest = thresholds.latest(args.ai_dir, args.corpus)
    base = RetrievalConfig.from_dict(
        {**latest.as_dict(), "dense_min_score": None, "keyword_min_score": None}
    )
    outcomes = evaluation.evaluate(store, chosen, base, progress.show)
    overrides = {
        "dense": args.dense_min_score,
        "keyword": args.keyword_min_score,
    }
    found = thresholds.establish(outcomes, overrides)
    for name, item in found.items():
        print(
            f"{name}: threshold {item['threshold']}, keeps {item['hits_kept']} of "
            f"{item['expected_hits']} expected hits, stops {item['negatives_no_match']} "
            f"of {item['negatives']} negative questions"
        )
        if item["set_by"] == "owner":
            print(
                f"  set by the owner; the retention rule gives {item['computed_threshold']}"
            )
        if item["conflict"]:
            print("  CONFLICT: the negative goal is not met; the owner decides.")
    if not writing(args):
        print(uc.NOT_WRITTEN)
        return 0
    record = evaluation.run_record(
        store, args.corpus, qs.TUNE, outcomes, question_set, base, None
    )
    runs.save_run(args.ai_dir, record)
    meta = {
        "created": record["created"],
        "tuning_run_id": record["run_id"],
        "question_set": record["question_set"],
        "build_id": record["build_id"],
    }
    number, _ = thresholds.write_version(args.ai_dir, args.corpus, base, found, meta)
    print(f"Retrieval config version {number} written.")
    finish(args)
    return 0


def snapshot(args: argparse.Namespace) -> object:
    manifest = st.read_manifest(args.corpus, args.corpus_dir)
    if args.corpus == "curated":
        return iq.unit_snapshot(manifest)
    selection = manifest["selection"]
    return {
        "article_count": selection["article_count"],
        "id_list_hash": selection["id_list_hash"],
    }


def cmd_accept(args: argparse.Namespace) -> int:
    record = st.read_record(args.corpus, args.corpus_dir)
    build_id = record["build_id"]
    read = state.holdout_read(args.ai_dir, args.corpus, build_id)
    if read is None:
        print(f"Build {build_id} has no holdout run: run evaluate --split holdout.")
        return 1
    run = runs.load_run(args.ai_dir, args.corpus, read["run_id"])
    question_set = load_questions(args)
    if run is None or run["question_set"]["hash"] != question_set.set_hash:
        print(
            "The question set changed since the holdout run, so the run does not count."
        )
        return 1
    report = iq.check(args.corpus, args.corpus_dir, args.model_dir, progress.show)
    event = state.designated_event(args.ai_dir, args.corpus)
    previous = (
        runs.load_run(args.ai_dir, args.corpus, event["evaluation_run_id"])
        if event
        else None
    )
    decision = acceptance.decide(run, previous, report.problems)
    print(f"Build {build_id}, holdout run {run['run_id']}")
    for item in decision.checks:
        mark = {True: "PASS", False: "FAIL", None: "n/a"}[item["passed"]]
        print(f"  {mark} {item['check']}: {item['detail']}")
    if not decision.ok:
        print("The build cannot be designated.")
        return 1
    approver = (args.approver or "").strip()
    if not approver and uc.is_interactive():
        approver = uc.ask("Approver name: ")
    if not approver:
        print(
            "The owner approves a designation: no name given, so nothing is designated."
        )
        return 1
    if not writing(args):
        print(uc.NOT_WRITTEN)
        return 0
    body = {
        "build_id": build_id,
        "collection": record["collection"],
        "corpus_version": record["corpus_version"],
        "corpus_fingerprint": record["corpus_fingerprint"],
        "mode": record["mode"],
        "designated_on": today(),
        "approver": approver,
        "evaluation_run_id": run["run_id"],
        "question_set": run["question_set"],
        "config": run["config"],
        "config_version": run["config_version"],
        "checks": decision.checks,
        "unit_snapshot": snapshot(args),
    }
    designated = state.designate(args.ai_dir, args.corpus, body)
    print(f"Designated build {build_id} (event {designated['event_hash'][:12]}).")
    runs.save_check(
        args.ai_dir, args.corpus, report, freshness.check(args.corpus, args.corpus_dir)
    )
    finish(args)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    current = state.read(args.ai_dir, args.corpus)
    for label in ("designated", "previous"):
        event = current[label]
        print(
            f"{label}: "
            + (
                f"build {event['build_id']} on {event['designated_on']}, "
                f"approved by {event['approver']}"
                if event
                else "none"
            )
        )
    print(f"holdout reads: {len(current['holdout_reads'])}")
    print(f"retrieval config versions: {thresholds.versions(args.ai_dir, args.corpus)}")
    for problem in state.verify(args.ai_dir, args.corpus):
        print(f"  FAIL {problem}")
    return 0


def step(args: argparse.Namespace, handler, **changes) -> int:
    return handler(argparse.Namespace(**{**vars(args), **changes}))


def waiting_questions(args: argparse.Namespace) -> list[qs.Question]:
    return [q for q in load_questions(args).questions if q.status != "approved"]


def cmd_all(args: argparse.Namespace) -> int:
    """Every step in order; a decision of the owner is never taken for the owner."""
    ai_dir, corpus = args.ai_dir, args.corpus
    print("== step: check ==")
    code = cmd_check(args)
    if code:
        print(f"Stopped at step check (exit code {code}).")
        return code
    waiting = waiting_questions(args)
    if waiting:
        print("== step: approve-questions ==")
        if args.approver or uc.is_interactive():
            code = step(args, cmd_approve_questions, all_pending=True)
            if code:
                return code
            waiting = waiting_questions(args)
        else:
            print(f"{len(waiting)} question(s) wait for approval; no approver name.")
    for name, handler, changes in (
        ("evaluate (tune)", cmd_evaluate, {"split": qs.TUNE}),
        ("thresholds", cmd_thresholds, {}),
    ):
        print(f"== step: {name} ==")
        before = thresholds.versions(ai_dir, corpus)
        code = step(args, handler, **changes)
        if code:
            print(f"Stopped at step {name} (exit code {code}).")
            return code
    if args.dry_run:
        print("Dry run: nothing written; the holdout and the acceptance are not run.")
        return 0
    found = thresholds.versions(ai_dir, corpus)
    if found == before:
        print("No retrieval config version was written, so the run stops here.")
        return 1
    data = thresholds.read_version(ai_dir, corpus, found[-1])
    conflicts = [n for n, t in data["thresholds"].items() if t["conflict"]]
    if conflicts:
        print(
            f"Stopped: the threshold goals conflict for {conflicts}; the owner decides."
        )
        return 1
    if waiting:
        print(
            f"{len(waiting)} question(s) wait for approval: no holdout, no acceptance."
        )
        return 0
    build_id, _ = candidate(args)
    if state.holdout_read(ai_dir, corpus, build_id):
        print(f"The holdout was already read for build {build_id}; its run is used.")
    else:
        print("== step: evaluate (holdout) ==")
        code = step(args, cmd_evaluate, split=qs.HOLDOUT)
        if code or not state.holdout_read(ai_dir, corpus, build_id):
            print("Stopped: the holdout was not read.")
            return code
    print("== step: accept ==")
    return cmd_accept(args)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", nargs="?", choices=COMMANDS, default="all")
    parser.add_argument("--corpus", choices=CORPORA, default="curated")
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--model-dir", type=Path, default=emb.DEFAULT_MODEL_DIR)
    parser.add_argument("--root", type=Path, default=uc.ROOT)
    parser.add_argument("--split", choices=(qs.TUNE, qs.HOLDOUT), default=qs.TUNE)
    parser.add_argument("--config-version", type=int, default=None)
    parser.add_argument(
        "--dense-min-score",
        type=float,
        default=None,
        help="thresholds step: use this dense threshold instead of the rule",
    )
    parser.add_argument(
        "--keyword-min-score",
        type=float,
        default=None,
        help="thresholds step: use this keyword threshold instead of the rule",
    )
    parser.add_argument("--approver", default=None)
    parser.add_argument("--id", action="append")
    parser.add_argument("--all-pending", action="store_true")
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    args.ai_dir = args.corpus_dir.parent
    handler = {
        "all": cmd_all,
        "check": cmd_check,
        "approve-questions": cmd_approve_questions,
        "evaluate": cmd_evaluate,
        "thresholds": cmd_thresholds,
        "accept": cmd_accept,
        "status": cmd_status,
    }[args.command]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
