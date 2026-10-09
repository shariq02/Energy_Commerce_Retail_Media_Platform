"""Run the knowledge corpus steps in order.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Steps: units (build), authored (written bodies, skipped when there is no index),
tokens (size check), wikipedia_fetch (the selected articles and edges from
Databricks; each part only when its local files are missing or --refresh-wikipedia
is given, and when the Databricks settings are in .env), wikipedia (corpus manifest); stops at the first failure. It first runs every step as a
dry run and prints the plan. In an interactive terminal it then asks whether to
apply (y or yes), runs the steps again to write, and asks what to approve.
--apply skips the question; without a terminal nothing is written.

Usage (repository root):
    python -m scripts.knowledge.run_all [--apply]
"""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter
from pathlib import Path

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from scripts.knowledge import author_units as auth
from scripts.knowledge import build_units as bu
from scripts.knowledge import report_tokens as rt
from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_fetch as wf


def write_mode(args: argparse.Namespace) -> list[str]:
    return ["--apply"] if args.apply else ["--dry-run"]


def authored_index(args: argparse.Namespace) -> Path:
    return args.authored_dir / auth.INDEX_NAME


def step_units(args: argparse.Namespace) -> int:
    argv = ["--root", str(args.root), "--corpus-dir", str(args.corpus_dir)]
    return bu.main([*argv, *write_mode(args)])


def step_authored(args: argparse.Namespace) -> int:
    if not authored_index(args).is_file():
        print("No authored units yet, so this step is skipped.")
        return 0
    argv = [
        "--authored-dir",
        str(args.authored_dir),
        "--corpus-dir",
        str(args.corpus_dir),
    ]
    if args.local_sources is not None:
        argv += ["--local-sources", str(args.local_sources)]
    return auth.main([*argv, *write_mode(args)])


def planned_bodies(args: argparse.Namespace) -> dict[str, str]:
    """The bodies the units and authored steps would write."""
    bodies = {unit["unit_id"]: unit["body"] for unit in bu.derived_units(args.root)}
    if authored_index(args).is_file():
        local = uc.load_local_sources(args.local_sources or uc.LOCAL_SOURCES_FILE)
        for unit in auth.authored_units(args.authored_dir, local):
            bodies[unit["unit_id"]] = unit["body"]
    return bodies


def step_tokens(args: argparse.Namespace) -> int:
    tokenizer = args.tokenizer or rt.DEFAULT_TOKENIZER
    if not args.apply:
        return rt.report(tokenizer, planned_bodies(args))
    if not uc.scan_units(args.corpus_dir):
        print("No units yet, so the token check is skipped.")
        return 0
    argv = ["--corpus-dir", str(args.corpus_dir), "--tokenizer", str(tokenizer)]
    return rt.main(argv)


def wiki_dir(args: argparse.Namespace) -> Path:
    return args.corpus_dir / "wikipedia"


def has_articles(args: argparse.Namespace) -> bool:
    return any((wiki_dir(args) / wc.SHARD_DIR_NAME).glob("shard_*.jsonl"))


def has_edges(args: argparse.Namespace) -> bool:
    return (wiki_dir(args) / wc.EDGES_NAME).is_file()


def missing_parts(args: argparse.Namespace) -> list[str]:
    """The Wikipedia parts to fetch: every part on refresh, else the missing ones."""
    local = {"articles": has_articles(args), "edges": has_edges(args)}
    return [p for p in wf.PARTS if args.refresh_wikipedia or not local[p]]


def step_wikipedia_fetch(args: argparse.Namespace) -> int:
    if not (wiki_dir(args) / wc.TERMS_NAME).is_file():
        print("No Wikipedia selection terms, so this step is skipped.")
        return 0
    parts = missing_parts(args)
    if not parts:
        print(
            "Wikipedia articles and edges are already local. Use --refresh-wikipedia."
        )
        return 0
    if wf.connection_settings() is None:
        print("Databricks settings are not all set in .env, so this step is skipped.")
        return 0
    argv = ["--wiki-dir", str(wiki_dir(args))]
    for part in parts:
        argv += ["--part", part]
    return wf.main([*argv, *write_mode(args)])


def step_wikipedia(args: argparse.Namespace) -> int:
    if not has_articles(args):
        print("No Wikipedia articles are local yet, so this step is skipped.")
        return 0
    return wc.main(["build", "--wiki-dir", str(wiki_dir(args)), *write_mode(args)])


STEPS = [
    ("units", step_units),
    ("authored", step_authored),
    ("tokens", step_tokens),
    ("wikipedia_fetch", step_wikipedia_fetch),
    ("wikipedia", step_wikipedia),
]


KIND_CHOICES = {"2": "metric", "3": "contract", "4": "rule"}


def ask(question: str) -> str:
    return input(question).strip()


def is_interactive() -> bool:
    return sys.stdin.isatty()


def ask_apply() -> bool:
    try:
        return uc.is_yes(ask("Apply these changes? (y/yes or n/no): "))
    except EOFError:
        return False


def print_approval_hint() -> None:
    print("Approval is your decision. To approve:")
    print("  python -m scripts.knowledge.approve_units \\")
    print('      --approver "<name>" --kind <kind>')
    print("  (add --apply to write; --all-pending selects every waiting unit)")


def approval_step(args: argparse.Namespace, waiting: list[uc.Unit]) -> None:
    counts = Counter(unit.kind for unit in waiting)
    print("Approve units now?")
    print(f"  1  every waiting unit ({len(waiting)})")
    for number, kind in KIND_CHOICES.items():
        print(f"  {number}  {kind} units ({counts[kind]} waiting)")
    print("  5  do not approve")
    try:
        choice = ask("Choice (1, 2, 3, 4 or 5): ")
    except EOFError:
        choice = "5"
    if choice == "1":
        chosen = waiting
    elif choice in KIND_CHOICES:
        chosen = [unit for unit in waiting if unit.kind == KIND_CHOICES[choice]]
    else:
        print("No approval.")
        return
    if not chosen:
        print("No unit of that kind waits for approval.")
        return
    approver = (args.approver or "").strip() or ask("Approver name: ")
    if not approver:
        print("No name given, so no approval.")
        return
    manifest = au.approve_chosen(args.corpus_dir, chosen, approver)
    print(f"Approved {len(chosen)} unit(s) as {approver}.")
    print(f"Manifest approval: {manifest['approval']}")


def wikipedia_approval_step(args: argparse.Namespace) -> None:
    """Offer the corpus-level approval of the Wikipedia selection rule and id list."""
    path = wiki_dir(args) / wc.MANIFEST_NAME
    if not path.is_file():
        return
    manifest = wc.read_previous(wiki_dir(args))
    status = manifest["approval"]["status"]
    if status == "approved":
        return
    selection = manifest["selection"]
    print(
        f"Wikipedia corpus approval is {status}: rule version "
        f"{selection['rule_version']}, {selection['article_count']} articles."
    )
    if not is_interactive():
        print("  python -m scripts.knowledge.wikipedia_corpus approve --approver NAME")
        return
    try:
        if not uc.is_yes(ask("Approve the Wikipedia corpus now? (y/yes or n/no): ")):
            return
        approver = (args.approver or "").strip() or ask("Approver name: ")
    except EOFError:
        return
    if approver:
        argv = ["approve", "--approver", approver, "--wiki-dir", str(wiki_dir(args))]
        wc.main([*argv, "--apply"])


def run_steps(args: argparse.Namespace) -> int:
    for name, step in STEPS:
        print(f"== step: {name} ==")
        code = step(args)
        if code != 0:
            print(f"Stopped at step {name} (exit code {code}).")
            return code
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=Path, default=uc.ROOT)
    parser.add_argument("--corpus-dir", type=Path, default=None)
    parser.add_argument("--authored-dir", type=Path, default=auth.AUTHORED_DIR)
    parser.add_argument("--local-sources", type=Path, default=None)
    parser.add_argument("--tokenizer", type=Path, default=None)
    parser.add_argument("--approver", default=None, help="name for the approval")
    parser.add_argument(
        "--refresh-wikipedia",
        action="store_true",
        help="fetch the Wikipedia articles again even when local files exist",
    )
    parser.add_argument("--apply", action="store_true", help="write without asking")
    args = parser.parse_args(argv)
    if args.corpus_dir is None:
        args.corpus_dir = args.root / "ai" / "knowledge_corpus"

    code = run_steps(copy.copy(args))
    if code != 0:
        return code
    if not args.apply:
        if not (is_interactive() and ask_apply()):
            print("Dry run: nothing was written. Use --apply, or answer y, to write.")
            return 0
        args.apply = True
        code = run_steps(args)
        if code != 0:
            return code
    print("== done ==")
    units = uc.scan_units(args.corpus_dir)
    waiting = [unit for unit in units if au.needs_approval(unit)]
    print(f"Units: {len(units)}, waiting for approval: {len(waiting)}")
    if waiting and is_interactive():
        approval_step(args, waiting)
    elif waiting:
        print_approval_hint()
    wikipedia_approval_step(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
