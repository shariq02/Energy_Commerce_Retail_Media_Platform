# Tahoe-100M metadata: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the Tahoe-100M metadata tables (cell lines, drugs, genes,
# samples) and the first pseudobulk files up to the size cap, and stage them in
# data/staging/tahoe/; each downloaded file is deleted after its parse.

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage
from _genomic_formats import read_parquet, stringify_nested

from config import get_logger

logger = get_logger(__name__)

TAHOE = "https://huggingface.co/datasets/tahoebio/Tahoe-100M/resolve/main/metadata"
TABLES = ["cell_line_metadata", "drug_metadata", "gene_metadata", "sample_metadata"]
PSEUDOBULK = "pseudobulk_differential_expression"
PSEUDOBULK_FILES = 1026


def parse_metadata(dataset: str):
    def parse(path: Path, tag: str):
        for frame in read_parquet(path):
            yield {dataset: stringify_nested(frame)}, len(frame)

    return parse


def main() -> None:
    for table in TABLES:
        stage(
            "tahoe",
            [table],
            [("", f"{TAHOE}/{table}.parquet", f"{table}.parquet")],
            parse_metadata(table),
        )
    stage(
        "tahoe",
        [PSEUDOBULK],
        [
            (
                "",
                f"{TAHOE}/{PSEUDOBULK}/train-{n:05d}-of-{PSEUDOBULK_FILES:05d}.parquet",
                f"{PSEUDOBULK}_{n:05d}.parquet",
            )
            for n in range(PSEUDOBULK_FILES)
        ],
        parse_metadata(PSEUDOBULK),
        capped=(PSEUDOBULK,),
    )
    logger.info("Tahoe metadata staged.")


if __name__ == "__main__":
    main()
