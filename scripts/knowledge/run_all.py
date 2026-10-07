"""Run the knowledge corpus steps in order.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Purpose: one command for the whole sequence, so the order does not have to be
remembered. Each step runs in this process; the run stops at the first step
that fails and returns that step's exit code. Steps, in order:

    units   convert the sources into units and rebuild the manifest
    tokens  check that every unit fits the embedding model's input size

Approval is the owner's decision. After the steps, with --apply and an
interactive terminal, the run asks what to approve: 1 (all), 2 (metrics),
3 (contracts), 4 (rules) or 5 (no approval). Nothing is approved without that
answer and an approver name (--approver, or asked). Without --apply, or when
the terminal is not interactive, nothing is approved and the approval command
is printed.

Dry run by default: with no --apply the steps write nothing.

Usage (from the repository root):
    python -m scripts.knowledge.run_all             # dry run
    python -m scripts.knowledge.run_all --apply     # write, then ask about approval
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from scripts.knowledge import build_units as bu
from scripts.knowledge import report_tokens as rt


def step_units(args: argparse.Namespace) -> int:
    argv = ["--root", str(args.root), "--corpus-dir", str(args.corpus_dir)]
    if args.apply:
        argv.append("--apply")
    return bu.main(argv)


def step_tokens(args: argparse.Namespace) -> int:
    if not uc.scan_units(args.corpus_dir):
        print("No units yet, so the token check is skipped. Run with --apply.")
        return 0
    argv = ["--corpus-dir", str(args.corpus_dir)]
    if args.tokenizer is not None:
        argv += ["--tokenizer", str(args.tokenizer)]
    return rt.main(argv)


STEPS = [("units", step_units), ("tokens", step_tokens)]


KIND_CHOICES = {"2": "metric", "3": "contract", "4": "rule"}


def ask(question: str) -> str:
    return input(question).strip()


def is_interactive() -> bool:
    return sys.stdin.isatty()


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=Path, default=uc.ROOT)
    parser.add_argument("--corpus-dir", type=Path, default=None)
    parser.add_argument("--tokenizer", type=Path, default=None)
    parser.add_argument("--approver", default=None, help="name for the approval")
    parser.add_argument("--apply", action="store_true", help="write the units")
    args = parser.parse_args(argv)
    if args.corpus_dir is None:
        args.corpus_dir = args.root / "ai" / "knowledge_corpus"

    for name, step in STEPS:
        print(f"== step: {name} ==")
        code = step(args)
        if code != 0:
            print(f"Stopped at step {name} (exit code {code}).")
            return code
    print("== done ==")
    units = uc.scan_units(args.corpus_dir)
    waiting = [unit for unit in units if au.needs_approval(unit)]
    print(f"Units: {len(units)}, waiting for approval: {len(waiting)}")
    if waiting and args.apply and is_interactive():
        approval_step(args, waiting)
    elif waiting:
        print_approval_hint()
    if not args.apply:
        print("Dry run: nothing was written. Run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
