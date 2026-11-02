# Genomics pharmacogenomics, expression and cancer sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the PharmGKB primary files, the GTEx median gene
# expression per tissue and the TCGA MC3 public somatic mutations, and stage
# them in data/staging/<source>/; each downloaded file is deleted after its
# parse.

import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage, table_parser
from _genomic_formats import read_table

from config import get_logger

logger = get_logger(__name__)

PHARMGKB = "https://s3.pgkb.org/data"
PHARMGKB_FILES = ["genes", "drugs", "variants", "relationships"]
GTEX_URL = (
    "https://storage.googleapis.com/adult-gtex/bulk-gex/v10/rna-seq/"
    "GTEx_Analysis_v10_RNASeQCv2.4.2_gene_median_tpm.gct.gz"
)
# TCGA MC3 public MAF (mc3.v0.2.8.PUBLIC.maf.gz), GRCh37 coordinates.
MC3_URL = "https://api.gdc.cancer.gov/data/1c8cfe5f-e52d-41ba-94da-f15ea1337efc"
MC3_COLUMNS = {
    "Hugo_Symbol",
    "Entrez_Gene_Id",
    "NCBI_Build",
    "Chromosome",
    "Start_Position",
    "End_Position",
    "Strand",
    "Variant_Classification",
    "Variant_Type",
    "Reference_Allele",
    "Tumor_Seq_Allele1",
    "Tumor_Seq_Allele2",
    "dbSNP_RS",
    "Tumor_Sample_Barcode",
    "HGVSc",
    "HGVSp_Short",
    "Transcript_ID",
    "Exon_Number",
    "t_depth",
    "t_ref_count",
    "t_alt_count",
    "Gene",
    "IMPACT",
    "FILTER",
}


def parse_pharmgkb(path: Path, tag: str):
    member = f"{tag}.tsv"
    with zipfile.ZipFile(path) as archive:
        extracted = Path(archive.extract(member, path.parent))
    try:
        for frame in read_table(extracted):
            yield {tag: frame}, len(frame)
    finally:
        extracted.unlink()


def main() -> None:
    for name in PHARMGKB_FILES:
        stage(
            "pharmgkb",
            [name],
            [(name, f"{PHARMGKB}/{name}.zip", f"{name}.zip")],
            parse_pharmgkb,
        )
    stage(
        "gtex",
        ["gene_median_tpm"],
        [("", GTEX_URL, GTEX_URL.rsplit("/", 1)[-1])],
        table_parser("gene_median_tpm", skip_lines=2),
    )
    stage(
        "tcga",
        ["mc3_somatic_mutations"],
        [("", MC3_URL, "mc3.v0.2.8.PUBLIC.maf.gz")],
        table_parser("mc3_somatic_mutations", usecols=lambda c: c in MC3_COLUMNS),
    )
    logger.info("Genomics pharmacogenomics, expression and cancer sources staged.")


if __name__ == "__main__":
    main()
