# Genomics variant score sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download AlphaMissense, the MAPS embedding metadata and CADD (rows
# at the ClinVar positions), and stage them in data/staging/<source>/; each
# downloaded file is deleted after its parse.

import gzip
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import clinvar_positions, stage, table_parser, unpack
from _genomic_formats import read_parquet, stringify_nested

from config import get_logger

logger = get_logger(__name__)

ALPHAMISSENSE_URL = (
    "https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz"
)
MAPS = (
    "https://huggingface.co/datasets/ching-goodfire/"
    "MAPS-ClinVar-VKS-Embeddings-L80-shuffled/resolve/main/metadata"
)
MAPS_FILES = [("test", "test-00000-of-00001.parquet")] + [
    ("train", f"train-{n:05d}-of-00008.parquet") for n in range(8)
]
CADD_URL = (
    "https://huggingface.co/datasets/genomics-commons/cadd-scores/"
    "resolve/main/cadd.sqlite.gz"
)
CADD_QUERY_POSITIONS = 900


def parse_maps(path: Path, tag: str):
    for frame in read_parquet(path, chunksize=5_000):
        frame.insert(0, "split_file", tag)
        yield {"metadata": stringify_nested(frame)}, len(frame)


def parse_cadd(path: Path, tag: str):
    positions = clinvar_positions()
    database = path.with_suffix("")
    with gzip.open(path, "rb") as source:
        if not unpack(source, database):
            raise RuntimeError("cadd: not staged, the disk is nearly full")
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        version = connection.execute("SELECT * FROM database_versions").fetchall()
        logger.info(f"CADD database_versions: {version}")
        sample = connection.execute("SELECT chrom FROM cadd_scores LIMIT 1").fetchone()
        prefixed = bool(sample and str(sample[0]).startswith("chr"))
        for chromosome, wanted in positions.items():
            name = f"chr{chromosome}" if prefixed else chromosome
            for at in range(0, len(wanted), CADD_QUERY_POSITIONS):
                part = wanted[at : at + CADD_QUERY_POSITIONS].tolist()
                marks = ",".join("?" * len(part))
                frame = pd.read_sql_query(
                    "SELECT chrom, pos, ref, alt, phred FROM cadd_scores "
                    f"WHERE chrom = ? AND pos IN ({marks})",
                    connection,
                    params=[name, *part],
                )
                yield {"scores": frame}, len(part)
            logger.info(f"cadd: chromosome {chromosome} done")
    finally:
        connection.close()
        database.unlink(missing_ok=True)


def main() -> None:
    stage(
        "alphamissense",
        ["variant_scores"],
        [("", ALPHAMISSENSE_URL, "AlphaMissense_hg38.tsv.gz")],
        table_parser("variant_scores"),
        capped=("variant_scores",),
    )
    stage(
        "maps_embeddings",
        ["metadata"],
        [(tag, f"{MAPS}/{name}", name) for tag, name in MAPS_FILES],
        parse_maps,
        capped=("metadata",),
    )
    stage(
        "cadd",
        ["scores"],
        [("v1.7", CADD_URL, "cadd.sqlite.gz")],
        parse_cadd,
    )
    logger.info("Genomics variant score sources staged.")


if __name__ == "__main__":
    main()
