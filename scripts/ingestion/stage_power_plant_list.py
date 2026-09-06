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
# The BNetzA CSV puts a variable number of title / disclaimer rows above the
# real header (one of which is a quoted cell spanning several physical lines)
# and can carry footnote rows below the data -- the counts drift between
# editions, so detect the header by content instead of hardcoding a skip.
# Tokens must match the CURRENT column set (Datensatztyp / EinheitMastrNummer /
# Anlagenbetreiber / Energietraeger / Nettonennleistung_MW / Kraftwerksstatus).
_HEADER_HINT_TOKENS = (
    "datensatztyp",
    "einheitmastrnummer",
    "anlagenbetreiber",
    "energietraeger",
    "nettonennleistung",
    "kraftwerksstatus",
    "bruttoleistung",
)


def _read_bnetza_csv(path: Path, scan: int = 30) -> tuple[pd.DataFrame, int]:
    """Read the whole file with no header (pandas resolves the multi-line quoted
    preamble cell), find the header row by matching >=2 hint tokens, then slice.
    Working purely in pandas' row index avoids the physical-line vs parsed-row
    mismatch that `skiprows` would hit on the embedded newline."""
    raw = pd.read_csv(
        path,
        sep=";",
        encoding=SOURCE_ENCODING,
        header=None,
        dtype=str,
        keep_default_na=False,
    )
    header_idx = None
    for i in range(min(scan, len(raw))):
        cells = [str(c).strip().lower() for c in raw.iloc[i].tolist() if str(c).strip()]
        if sum(any(t in c for t in _HEADER_HINT_TOKENS) for c in cells) >= 2:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError(
            f"could not find the BNetzA header row in the first {scan} lines of "
            f"{path} -- inspect the file and update _HEADER_HINT_TOKENS."
        )
    cols = [str(c).strip().rstrip("*").strip() for c in raw.iloc[header_idx].tolist()]
    blank = sum(1 for c in cols if not c or c.lower() == "nan")
    if blank > 0.2 * len(cols):
        raise RuntimeError(
            f"header row {header_idx} in {path} has {blank}/{len(cols)} blank column "
            "names -- the detected row is not the real header."
        )
    df = raw.iloc[header_idx + 1 :].copy()
    df.columns = cols
    return df, header_idx


def stage_main_list() -> tuple[int, Path]:
    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "kraftwerksliste.csv"
    src = RAW_DIR / "Kraftwerksliste_CSV.csv"

    df, header_row = _read_bnetza_csv(src)
    # Empty strings read as "" (keep_default_na=False) -> treat as missing.
    df = df.replace("", pd.NA)
    # Drop trailing footnote / fully-empty rows: every key column empty, or the
    # first column is a disclaimer starting with a symbol / "Stand" / a note.
    name_col = df.columns[0]
    foot = (
        df.drop(columns=[name_col]).isna().all(axis=1)
        | df[name_col].isna()
        | df[name_col]
        .astype(str)
        .str.strip()
        .str.match(r"^(\*|\(|\[|Stand|Quelle|Hinweis|\d+\))")
    )
    df = df[~foot]
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(
        f"Staged analytical/kraftwerksliste.csv -- {len(df)} rows, "
        f"{len(df.columns)} columns (header row {header_row}, "
        f"{int(foot.sum())} footnote/empty rows dropped)"
    )
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
