# Genomics reference sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download UniProt Swiss-Prot and the RefSeq and GENCODE gene
# annotations, keep the human entries and the gene and transcript rows, and
# stage them in data/staging/<source>/; each downloaded file is deleted after
# its parse.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage
from _genomic_formats import (
    GTF_ATTRIBUTE_PATTERN,
    GTF_COLUMNS,
    add_fields,
    read_table,
    read_uniprot,
)

from config import get_logger

logger = get_logger(__name__)

UNIPROT_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/current_release/"
    "knowledgebase/complete/uniprot_sprot.dat.gz"
)
REFSEQ_URL = (
    "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/genes/"
    "hg38.ncbiRefSeq.gtf.gz"
)
GENCODE_RELEASE = 50
GENCODE_URL = (
    "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/"
    f"release_{GENCODE_RELEASE}/gencode.v{GENCODE_RELEASE}.basic.annotation.gtf.gz"
)

HUMAN_TAX_ID = "9606"
UNIPROT_FEATURE_TYPES = {
    "DOMAIN",
    "REGION",
    "MOTIF",
    "ACT_SITE",
    "BINDING",
    "SITE",
    "DNA_BIND",
    "ZN_FING",
    "TRANSMEM",
    "SIGNAL",
    "MOD_RES",
    "VARIANT",
}
UNIPROT_ENTRY_COLUMNS = [
    "accession",
    "entry_name",
    "gene_name",
    "protein_name",
    "tax_id",
    "sequence_length",
]
UNIPROT_FEATURE_COLUMNS = ["accession", "feature_type", "location", "qualifiers"]
GTF_FEATURES = {"gene", "transcript", "exon", "CDS", "UTR"}
GTF_ATTRIBUTES = [
    "gene_id",
    "gene_name",
    "gene_type",
    "transcript_id",
    "transcript_name",
    "transcript_type",
]


def parse_uniprot(path: Path, tag: str):
    for entries, features, scanned in read_uniprot(
        path, HUMAN_TAX_ID, UNIPROT_FEATURE_TYPES
    ):
        yield (
            {
                "entries": pd.DataFrame(entries, columns=UNIPROT_ENTRY_COLUMNS),
                "features": pd.DataFrame(features, columns=UNIPROT_FEATURE_COLUMNS),
            },
            scanned,
        )


def parse_gtf(path: Path, tag: str):
    for frame in read_table(path, names=GTF_COLUMNS):
        read = len(frame)
        frame = frame.loc[frame["feature"].isin(GTF_FEATURES)].copy()
        yield (
            {
                "annotation": add_fields(
                    frame, "attributes", GTF_ATTRIBUTES, GTF_ATTRIBUTE_PATTERN
                )
            },
            read,
        )


def main() -> None:
    stage(
        "uniprot",
        ["entries", "features"],
        [("", UNIPROT_URL, "uniprot_sprot.dat.gz")],
        parse_uniprot,
    )
    stage(
        "refseq",
        ["annotation"],
        [("", REFSEQ_URL, "hg38.ncbiRefSeq.gtf.gz")],
        parse_gtf,
    )
    stage(
        "gencode",
        ["annotation"],
        [("", GENCODE_URL, GENCODE_URL.rsplit("/", 1)[-1])],
        parse_gtf,
    )
    logger.info("Genomics reference sources staged.")


if __name__ == "__main__":
    main()
