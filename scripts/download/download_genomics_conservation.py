# Genomics conservation sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the UCSC phyloP and phastCons bigWig files, read the score
# at each ClinVar position, and stage the values in data/staging/<source>/;
# each downloaded file is deleted after its parse. Needs pybigtools.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import clinvar_positions, stage
from _genomic_formats import BATCH_ROWS

from config import get_logger

logger = get_logger(__name__)

UCSC = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38"
BIGWIGS = {
    "phylop_100way": "phyloP100way/hg38.phyloP100way.bw",
    "phastcons_100way": "phastCons100way/hg38.phastCons100way.bw",
}
# Positions closer than WINDOW_GAP bases are read with one call.
WINDOW_GAP = 1_000
WINDOW_SPAN = 100_000


def windows(positions: list[int]) -> list[list[int]]:
    """Group sorted positions into runs that one bigWig call can read."""
    runs: list[list[int]] = []
    for position in positions:
        run = runs[-1] if runs else None
        if run and position - run[-1] <= WINDOW_GAP and position - run[0] < WINDOW_SPAN:
            run.append(position)
        else:
            runs.append([position])
    return runs


def _frame(rows: list[tuple[str, int, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["chromosome", "position", "score"])


def parse_bigwig(path: Path, tag: str):
    try:
        import pybigtools
    except ImportError as error:
        raise RuntimeError("pybigtools is not installed; install it first") from error

    positions = clinvar_positions()
    reader = pybigtools.open(str(path))
    try:
        sizes = reader.chroms()
        for chromosome, wanted in positions.items():
            name = "chrM" if chromosome == "MT" else f"chr{chromosome}"
            if name not in sizes:
                continue
            rows: list[tuple[str, int, float]] = []
            for run in windows(wanted.tolist()):
                values = reader.values(name, run[0] - 1, run[-1])
                rows += [(chromosome, p, values[p - run[0]]) for p in run]
                if len(rows) >= BATCH_ROWS:
                    yield {tag: _frame(rows)}, len(rows)
                    rows = []
            if rows:
                yield {tag: _frame(rows)}, len(rows)
            logger.info(f"{tag}: chromosome {chromosome} done")
    finally:
        close = getattr(reader, "close", None)
        if close:
            close()


def main() -> None:
    for dataset, file in BIGWIGS.items():
        stage(
            "ucsc_conservation",
            [dataset],
            [(dataset, f"{UCSC}/{file}", Path(file).name)],
            parse_bigwig,
        )
    logger.info("Genomics conservation sources staged.")


if __name__ == "__main__":
    main()
