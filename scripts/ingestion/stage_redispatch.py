# Redispatch staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: stage the Redispatch 2013-2020 archive in
# data/raw/redispatch/ as the single logical redispatch_measures
# dataset, written to data/staging/redispatch/. Local file operations
# only -- does not upload anywhere. data/raw/ is read-only throughout.
#
# The current publication (~Oct 2023 -> present) is not yet acquired,
# so this dataset holds only the archive regime for now. A
# coverage_regime column is added so the current-publication rows can
# be unioned into this same logical dataset later without a schema
# change -- one dataset with an explicit regime attribute, not two.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_DIR = DATA_RAW_DIR / "redispatch"
STAGING_DIR = DATA_STAGING_DIR / "redispatch"
ANALYTICAL_DIR = STAGING_DIR / "analytical"


def stage_redispatch_measures() -> tuple[int, Path]:
    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "redispatch_measures.csv"

    archive_files = list((RAW_DIR / "archive_2013_2020").glob("*.csv"))
    if not archive_files:
        raise FileNotFoundError(
            f"No archive CSV found under {RAW_DIR / 'archive_2013_2020'}"
        )

    df = pd.read_csv(archive_files[0], sep=";", encoding="utf-8-sig")
    df.insert(0, "coverage_regime", "archive_2013_2020")
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Staged analytical/redispatch_measures.csv -- {len(df)} rows")
    return len(df), out_path


def main() -> None:
    rows, path = stage_redispatch_measures()

    logger.info("Redispatch staging complete.")
    logger.info(f"  redispatch_measures: {rows} rows -> {path}")
    logger.warning(
        "Current publication (~Oct 2023 -> present) not staged -- not yet acquired."
    )


if __name__ == "__main__":
    main()
