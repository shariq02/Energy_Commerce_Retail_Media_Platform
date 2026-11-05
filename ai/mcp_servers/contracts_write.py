"""Write the tool lock after the plan is shown and the owner answers y or yes.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The plan lists every new tool, every changed tool, every raised version and every
removed tool. A removed tool leaves the lock only with this answer: it is the owner's
approval. Nothing is written while the check finds an error. The `evaluated_version`
values already in the lock are kept.

Usage (repository root):
    python3 -m ai.mcp_servers.contracts_write
"""

from __future__ import annotations

import sys
from collections.abc import Callable

from ai.mcp_servers import contracts as ct


def main(ask: Callable[[str], str] = input) -> int:
    analysis = ct.analyse_repository()
    for text in analysis.errors:
        print("ERROR  ", text)
    if analysis.errors:
        print("STOP: nothing written; fix the errors above")
        return 1
    if not analysis.pending:
        print("the lock is current; nothing to write")
        return 0
    print(f"{ct.LOCK_FILE.name} will change:")
    for text in analysis.pending:
        print("  -", text)
    answer = ask(f"Write {ct.LOCK_FILE.name}? Answer y or yes: ")
    if answer.strip().lower() not in ("y", "yes"):
        print("nothing written")
        return 1
    ct.LOCK_FILE.write_text(ct.render_lock(analysis.lock), encoding="utf-8")
    print(f"written {ct.LOCK_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
