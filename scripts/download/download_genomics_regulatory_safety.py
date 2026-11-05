# Genomics regulatory, drug safety and benchmark sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the SCREEN cCRE registry, the last eight FAERS quarters and
# the long-range benchmark tasks (causal eQTL and pathogenic variants), and
# stage them in data/staging/<source>/; each downloaded file is deleted after
# its parse.

import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage, table_parser
from _genomic_formats import BATCH_ROWS, read_table

from config import get_logger

logger = get_logger(__name__)

CCRE_URL = "https://downloads.wenglab.org/Registry-V4/GRCh38-cCREs.bed"
CCRE_COLUMNS = [
    "chrom",
    "start",
    "end",
    "dhs_accession",
    "ccre_accession",
    "ccre_class",
]
FAERS = "https://fis.fda.gov/content/Exports/faers_ascii_{quarter}.zip"
FAERS_QUARTERS = ["2026q1", "2026q2"]
FAERS_DATASETS = ["demo", "drug", "reac", "outc", "rpsr", "ther", "indi"]
LONG_RANGE = (
    "https://huggingface.co/datasets/InstaDeepAI/genomics-long-range-benchmark/"
    "resolve/main"
)
LONG_RANGE_TASKS = {
    "causal_eqtl": "variant_effect_causal_eqtl/All_Tissues.csv",
    "pathogenic_coding": "variant_effect_pathogenic/vep_pathogenic_coding.csv",
    "pathogenic_non_coding": "variant_effect_pathogenic/vep_pathogenic_non_coding.csv",
}


def parse_faers(path: Path, tag: str):
    """One zip holds one `$`-separated file per FAERS table."""
    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            dataset = Path(member).stem[:-4].lower()
            if not member.lower().endswith(".txt") or dataset not in FAERS_DATASETS:
                continue
            text_file = path.with_name(f"{tag}_{dataset}.txt")
            with archive.open(member) as source, open(text_file, "wb") as out:
                out.write(source.read())
            try:
                for frame in read_table(text_file, sep="$"):
                    frame.insert(0, "quarter", tag)
                    yield {dataset: frame}, len(frame)
            finally:
                text_file.unlink(missing_ok=True)


def parse_csv(dataset: str):
    def parse(path: Path, tag: str):
        frames = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            chunksize=BATCH_ROWS,
        )
        for frame in frames:
            yield {dataset: frame}, len(frame)

    return parse


def main() -> None:
    stage(
        "encode",
        ["ccre_registry"],
        [("V4", CCRE_URL, "GRCh38-cCREs.bed")],
        table_parser("ccre_registry", names=CCRE_COLUMNS),
    )
    stage(
        "faers",
        FAERS_DATASETS,
        [
            (quarter, FAERS.format(quarter=quarter), f"faers_ascii_{quarter}.zip")
            for quarter in FAERS_QUARTERS
        ],
        parse_faers,
    )
    for task, file in LONG_RANGE_TASKS.items():
        stage(
            "long_range_benchmark",
            [task],
            [("", f"{LONG_RANGE}/{file}", Path(file).name)],
            parse_csv(task),
        )
    logger.info("Genomics regulatory and safety sources staged.")


if __name__ == "__main__":
    main()
