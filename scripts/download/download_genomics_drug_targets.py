# Genomics drug and target sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the ChEMBL compound structures and target mapping and the
# Open Targets datasets, and stage them in data/staging/<source>/; each
# downloaded file is deleted after its parse.

import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import REQUEST_TIMEOUT, stage, table_parser
from _genomic_formats import read_parquet, stringify_nested

from config import get_logger

logger = get_logger(__name__)

CHEMBL_RELEASE = "37"
CHEMBL = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest"
CHEMBL_TARGET_COLUMNS = [
    "uniprot_accession",
    "chembl_target_id",
    "target_name",
    "target_type",
]

OPEN_TARGETS_RELEASE = "26.09"
OPEN_TARGETS = (
    "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/"
    f"{OPEN_TARGETS_RELEASE}/output"
)
OPEN_TARGETS_BATCH_ROWS = 20_000
OPEN_TARGETS_DATASETS = [
    "association_overall_direct",
    "association_by_datasource_direct",
    "target",
    "disease",
    "drug_molecule",
    "drug_mechanism_of_action",
    "evidence_gwas_credible_sets",
]


def parquet_parts(dataset: str) -> list[str]:
    page = requests.get(f"{OPEN_TARGETS}/{dataset}/", timeout=REQUEST_TIMEOUT).text
    parts = sorted(set(re.findall(r'href="([^"?/][^"]*\.parquet)"', page)))
    if not parts:
        raise RuntimeError(f"opentargets/{dataset}: no Parquet files listed")
    return parts


def parse_open_targets(dataset: str):
    def parse(path: Path, tag: str):
        for frame in read_parquet(path, chunksize=OPEN_TARGETS_BATCH_ROWS):
            yield {dataset: stringify_nested(frame)}, len(frame)

    return parse


def main() -> None:
    stage(
        "chembl",
        ["compounds"],
        [
            (
                CHEMBL_RELEASE,
                f"{CHEMBL}/chembl_{CHEMBL_RELEASE}_chemreps.txt.gz",
                f"chembl_{CHEMBL_RELEASE}_chemreps.txt.gz",
            )
        ],
        table_parser("compounds"),
    )
    stage(
        "chembl",
        ["uniprot_mapping"],
        [
            (
                CHEMBL_RELEASE,
                f"{CHEMBL}/chembl_uniprot_mapping.txt",
                "chembl_uniprot_mapping.txt",
            )
        ],
        table_parser("uniprot_mapping", names=CHEMBL_TARGET_COLUMNS),
    )
    for dataset in OPEN_TARGETS_DATASETS:
        stage(
            "opentargets",
            [dataset],
            [
                (OPEN_TARGETS_RELEASE, f"{OPEN_TARGETS}/{dataset}/{part}", part)
                for part in parquet_parts(dataset)
            ],
            parse_open_targets(dataset),
            capped=(dataset,),
        )
    logger.info("Genomics drug and target sources staged.")


if __name__ == "__main__":
    main()
