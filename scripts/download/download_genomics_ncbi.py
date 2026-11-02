# Genomics NCBI sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download ClinVar, NCBI Gene, MedGen, GTR and dbVar files, keep the
# human GRCh38 rows and stage them in data/staging/<source>/; each downloaded
# file is deleted after its parse.

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage, table_parser
from _genomic_formats import (
    VCF_INFO_PATTERN,
    add_fields,
    read_table,
    read_table_prefix,
)

from config import get_logger

logger = get_logger(__name__)

NCBI = "https://ftp.ncbi.nlm.nih.gov"
CLINVAR = f"{NCBI}/pub/clinvar/tab_delimited"
GENE = f"{NCBI}/gene/DATA"
DBVAR = f"{NCBI}/pub/dbVar/data/Homo_sapiens/by_study/vcf"

# The current file first: it has the most columns; older snapshots are
# aligned to it.
CLINVAR_SNAPSHOTS = [
    ("current", f"{CLINVAR}/variant_summary.txt.gz", "variant_summary.txt.gz"),
    (
        "2026-01",
        f"{CLINVAR}/archive/variant_summary_2026-01.txt.gz",
        "variant_summary_2026-01.txt.gz",
    ),
    (
        "2025-01",
        f"{CLINVAR}/archive/variant_summary_2025-01.txt.gz",
        "variant_summary_2025-01.txt.gz",
    ),
]

DBVAR_FILES = [
    "nstd102.GRCh38.variant_region.vcf.gz",
    "nstd102.GRCh38.variant_call.vcf.gz",
    "estd214.GRCh38.variant_region.vcf.gz",
]
DBVAR_INFO_KEYS = ["SVTYPE", "END", "SVLEN"]


def not_suppressed(frame):
    return frame["SUPPRESS"] != "Y"


def parse_gene2refseq(path: Path, tag: str):
    for frame, scanned in read_table_prefix(path, "9606\t"):
        yield {"gene2refseq": frame}, scanned


def parse_dbvar(path: Path, tag: str):
    study, _, file_type = tag.split(".")
    for frame in read_table(path):
        frame = add_fields(frame, "INFO", DBVAR_INFO_KEYS, VCF_INFO_PATTERN)
        frame.insert(0, "file_type", file_type)
        frame.insert(0, "study", study)
        yield {"variants": frame}, len(frame)


def main() -> None:
    stage(
        "clinvar",
        ["variant_summary"],
        CLINVAR_SNAPSHOTS,
        table_parser(
            "variant_summary",
            keep=lambda f: f["Assembly"] == "GRCh38",
            tag_column="snapshot",
        ),
    )
    stage(
        "clinvar",
        ["variation_allele"],
        [("", f"{CLINVAR}/variation_allele.txt.gz", "variation_allele.txt.gz")],
        table_parser("variation_allele"),
    )
    stage(
        "ncbi_gene",
        ["gene_info"],
        [
            (
                "",
                f"{GENE}/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz",
                "Homo_sapiens.gene_info.gz",
            )
        ],
        table_parser("gene_info", keep=lambda f: f["tax_id"] == "9606"),
    )
    stage(
        "ncbi_gene",
        ["gene2refseq"],
        [("", f"{GENE}/gene2refseq.gz", "gene2refseq.gz")],
        parse_gene2refseq,
    )
    stage(
        "ncbi_gene",
        ["mim2gene_medgen"],
        [("", f"{GENE}/mim2gene_medgen", "mim2gene_medgen")],
        table_parser("mim2gene_medgen"),
    )
    for name, dataset in [("MGCONSO", "concepts"), ("MGREL", "relations")]:
        stage(
            "medgen",
            [dataset],
            [("", f"{NCBI}/pub/medgen/{name}.RRF.gz", f"{name}.RRF.gz")],
            table_parser(dataset, keep=not_suppressed, sep="|"),
        )
    stage(
        "gtr",
        ["test_condition_gene"],
        [
            (
                "",
                f"{NCBI}/pub/GTR/data/test_condition_gene.txt",
                "test_condition_gene.txt",
            )
        ],
        table_parser("test_condition_gene"),
    )
    stage(
        "dbvar",
        ["variants"],
        [
            (name.removesuffix(".vcf.gz"), f"{DBVAR}/{name}", name)
            for name in DBVAR_FILES
        ],
        parse_dbvar,
    )
    logger.info("Genomics NCBI sources staged.")


if __name__ == "__main__":
    main()
