# Power plant list (Kraftwerksliste) staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: consolidate the BNetzA Kraftwerksliste raw files in
# data/raw/kraftwerksliste/ into two logical staging datasets (the main
# power-plant list and the Zu-/Rueckbau planned-capacity summary),
# written to data/staging/kraftwerksliste/. Local file operations only
# -- does not upload anywhere. data/raw/ is read-only throughout.
#
# Both source files are small (<1 MB); no chunked writer is needed --
# chunking would be artificial here, same reasoning as stage_smard.py.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_DIR = DATA_RAW_DIR / "kraftwerksliste"
STAGING_DIR = DATA_STAGING_DIR / "kraftwerksliste"
ANALYTICAL_DIR = STAGING_DIR / "analytical"

SOURCE_ENCODING = "latin-1"
MAIN_LIST_HEADER_ROW = 7  # 7 metadata/title rows precede the real header


def stage_main_list() -> tuple[int, Path]:
    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "kraftwerksliste.csv"

    df = pd.read_csv(
        RAW_DIR / "Kraftwerksliste_CSV.csv",
        sep=";",
        encoding=SOURCE_ENCODING,
        skiprows=MAIN_LIST_HEADER_ROW,
    )
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Staged analytical/kraftwerksliste.csv -- {len(df)} rows")
    return len(df), out_path


def stage_zu_und_rueckbau() -> tuple[int, Path]:
    """Expected conventional-capacity additions 2026-2029 by energy source --
    a small pivot summary, not a per-plant list, so it stages as its own
    dataset rather than folding into the main list."""
    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "zu_und_rueckbau.csv"

    raw = pd.read_excel(RAW_DIR / "ZuUndRueckbau.xlsx", header=5)
    raw = raw.dropna(axis=1, how="all")
    raw.columns = ["energietraeger", "2026", "2027", "2028", "2029", "2026_2029_total"]
    df = raw.dropna(subset=["energietraeger"])
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Staged analytical/zu_und_rueckbau.csv -- {len(df)} rows")
    return len(df), out_path


def main() -> None:
    rows_main, path_main = stage_main_list()
    rows_zu, path_zu = stage_zu_und_rueckbau()

    logger.info("Kraftwerksliste staging complete.")
    logger.info(f"  kraftwerksliste: {rows_main} rows -> {path_main}")
    logger.info(f"  zu_und_rueckbau: {rows_zu} rows -> {path_zu}")


if __name__ == "__main__":
    main()
