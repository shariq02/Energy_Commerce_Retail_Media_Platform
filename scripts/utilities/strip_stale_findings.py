"""Remove STALE blocks from src/schemas/silver_findings/*.md.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Usage: python scripts/utilities/strip_stale_findings.py [--apply]
Dry run by default; lists each stale block, --apply rewrites the files.
"""

import re
import sys
from pathlib import Path

DIR = Path(__file__).resolve().parents[2] / "src/schemas/silver_findings"
BLOCK = re.compile(r"<!-- BEGIN (\S+) -->.*?<!-- END \1 -->\n*", re.DOTALL)


def main() -> None:
    apply = "--apply" in sys.argv
    for f in sorted(DIR.glob("*.md")):
        text = f.read_text(encoding="utf-8")
        gone = [m.group(1) for m in BLOCK.finditer(text) if "> STALE" in m.group(0)]
        if not gone:
            continue
        print(f"{f.name}: {len(gone)} stale block(s)")
        for g in gone:
            print(f"  - {g}")
        if apply:
            text = BLOCK.sub(
                lambda m: "" if "> STALE" in m.group(0) else m.group(0), text
            )
            f.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
