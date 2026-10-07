"""Report the token length of every knowledge unit.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Purpose: count the tokens of each unit body with the embedding model's own
tokenizer and report units that do not fit its input size. The limit counts the
two special tokens. A unit that fits is ``ok``. A unit that is longer but whose
every paragraph fits is ``split``. A unit with a paragraph that does not fit is
``fail``: it must be rewritten, because a paragraph is never truncated.

Read-only. The exit code is 1 when a unit is ``fail`` and 2 when the tokenizer
file is missing.

Usage (from the repository root):
    python -m scripts.knowledge.report_tokens
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.knowledge import _unit_common as uc

MAX_TOKENS = 256
DEFAULT_TOKENIZER = (
    Path.home()
    / ".cache"
    / "chroma"
    / "onnx_models"
    / "all-MiniLM-L6-v2"
    / "onnx"
    / "tokenizer.json"
)


def load_tokenizer(path: Path):
    from tokenizers import Tokenizer

    tokenizer = Tokenizer.from_file(str(path))
    tokenizer.no_truncation()
    tokenizer.no_padding()
    return tokenizer


def count_tokens(tokenizer, text: str) -> int:
    return len(tokenizer.encode(text).ids)


def classify(tokenizer, body: str) -> tuple[str, int, int]:
    """Return (status, whole-body tokens, longest-paragraph tokens)."""
    total = count_tokens(tokenizer, body)
    paragraphs = [part for part in body.split("\n\n") if part.strip()]
    longest = max((count_tokens(tokenizer, part) for part in paragraphs), default=0)
    if longest > MAX_TOKENS:
        return "fail", total, longest
    if total > MAX_TOKENS:
        return "split", total, longest
    return "ok", total, longest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tokenizer", type=Path, default=DEFAULT_TOKENIZER)
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    args = parser.parse_args(argv)
    if not args.tokenizer.is_file():
        print(f"ERROR: tokenizer file not found: {args.tokenizer}")
        return 2

    tokenizer = load_tokenizer(args.tokenizer)
    rows = []
    for unit in uc.scan_units(args.corpus_dir):
        status, total, longest = classify(tokenizer, unit.body)
        rows.append((unit.unit_id, status, total, longest))
    print(f"Tokenizer: {args.tokenizer}")
    print(f"Limit: {MAX_TOKENS} tokens including the two special tokens")
    print(f"Units: {len(rows)}")
    for status in ("ok", "split", "fail"):
        print(f"  {status}: {sum(1 for row in rows if row[1] == status)}")
    print("Longest units (unit_id, status, tokens, longest paragraph):")
    for unit_id, status, total, longest in sorted(rows, key=lambda r: -r[2])[:10]:
        print(f"  {unit_id}  {status}  {total}  {longest}")
    failed = [row for row in rows if row[1] == "fail"]
    for unit_id, _, total, longest in failed:
        print(f"FAIL {unit_id}: a paragraph has {longest} tokens (unit {total})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
