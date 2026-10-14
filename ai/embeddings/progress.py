"""Progress lines for the long steps of the embedding build.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

One line shows the share done, the time used, the time left (from the average
rate so far) and the rate. Library only.
"""

from __future__ import annotations

import time


def show(line: str) -> None:
    print(line, flush=True)


def minutes(seconds: float) -> str:
    return f"{seconds / 60:.1f} min"


def progress_line(
    label: str, done: int, total: int, started: float, unit: str = "chunks"
) -> str:
    used = max(time.monotonic() - started, 1e-9)
    rate = done / used
    left = (total - done) / rate if rate else 0.0
    return (
        f"  [{label}] {done}/{total} {unit} ({100 * done / max(total, 1):.1f}%) | "
        f"{minutes(used)} elapsed | about {minutes(left)} left | {rate:.0f} {unit}/s"
    )
