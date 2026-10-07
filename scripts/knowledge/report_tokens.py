"""Report the token length of every knowledge unit.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Counts the tokens of each body with the embedding model's tokenizer against the
256-token limit (special tokens included). A paragraph over the limit is "fail".
Read-only. Exit code 1 on fail, 2 when the tokenizer file is missing.

Usage (repository root):
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


def report(tokenizer_path: Path, bodies: dict[str, str]) -> int:
    """Print the size report for these bodies; return the exit code."""
    if not tokenizer_path.is_file():
        print(f"ERROR: tokenizer file not found: {tokenizer_path}")
        return 2
    tokenizer = load_tokenizer(tokenizer_path)
    rows = []
    for unit_id, body in sorted(bodies.items()):
        status, total, longest = classify(tokenizer, body)
        rows.append((unit_id, status, total, longest))
    print(f"Tokenizer: {tokenizer_path}")
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tokenizer", type=Path, default=DEFAULT_TOKENIZER)
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    args = parser.parse_args(argv)
    bodies = {unit.unit_id: unit.body for unit in uc.scan_units(args.corpus_dir)}
    return report(args.tokenizer, bodies)


if __name__ == "__main__":
    sys.exit(main())
