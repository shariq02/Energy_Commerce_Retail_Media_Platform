# Genomics curation sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download ClinGen gene validity and dosage sensitivity, the HPO
# ontology and annotations, the CPIC gene-drug pairs and the FDA table of
# pharmacogenomic biomarkers, and stage them in data/staging/<source>/; each
# downloaded file is deleted after its parse.

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import MANUAL, stage, table_parser

from config import get_logger

logger = get_logger(__name__)

CLINGEN_VALIDITY_URL = "https://search.clinicalgenome.org/kb/gene-validity/download"
CLINGEN_DOSAGE_URL = (
    "https://ftp.clinicalgenome.org/ClinGen_gene_curation_list_GRCh38.tsv"
)
HPO = (
    "https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download"
)
CPIC_PAIRS_URL = "https://api.cpicpgx.org/v1/pair_view"
# fda.gov blocks scripted requests: the table is exported by hand to Excel.
FDA_BIOMARKERS_URL = (
    f"{MANUAL}https://www.fda.gov/drugs/science-and-research-drugs/"
    "table-pharmacogenomic-biomarkers-drug-labeling"
)
# The validity CSV opens with 4 banner lines and a '+++' line under the header.
CLINGEN_VALIDITY_SKIP = [0, 1, 2, 3, 5]


def text_frame(records: list[dict]) -> pd.DataFrame:
    """Records as text; lists and objects are kept as JSON."""
    frame = pd.DataFrame(records)
    for column in frame.columns:
        frame[column] = frame[column].map(
            lambda v: json.dumps(v) if isinstance(v, list | dict) else v
        )
    return frame


def parse_clingen_validity(path: Path, tag: str):
    frame = pd.read_csv(
        path,
        skiprows=CLINGEN_VALIDITY_SKIP,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    yield {"gene_validity": frame}, len(frame)


def parse_hpo_ontology(path: Path, tag: str):
    graph = json.loads(path.read_text(encoding="utf-8"))["graphs"][0]
    terms = [
        {
            "id": node["id"],
            "label": node.get("lbl"),
            "definition": node.get("meta", {}).get("definition", {}).get("val"),
            "deprecated": node.get("meta", {}).get("deprecated", False),
        }
        for node in graph["nodes"]
        if node.get("type") == "CLASS" and "/HP_" in node["id"]
    ]
    yield (
        {"terms": text_frame(terms), "term_relations": text_frame(graph["edges"])},
        len(graph["nodes"]) + len(graph["edges"]),
    )


def parse_cpic_pairs(path: Path, tag: str):
    records = json.loads(path.read_text(encoding="utf-8"))
    yield {"pairs": text_frame(records)}, len(records)


def parse_fda_biomarkers(path: Path, tag: str):
    # Row 1 of the browser export is the page title; the header is row 2.
    frame = pd.read_excel(
        path, skiprows=1, dtype=str, keep_default_na=False, na_values=[""]
    )
    yield {"pgx_biomarkers": frame}, len(frame)


def main() -> None:
    stage(
        "clingen",
        ["gene_validity"],
        [("", CLINGEN_VALIDITY_URL, "gene_validity.csv")],
        parse_clingen_validity,
    )
    stage(
        "clingen",
        ["dosage_sensitivity"],
        [("", CLINGEN_DOSAGE_URL, "ClinGen_gene_curation_list_GRCh38.tsv")],
        table_parser("dosage_sensitivity"),
    )
    stage(
        "hpo",
        ["terms", "term_relations"],
        [("", f"{HPO}/hp.json", "hp.json")],
        parse_hpo_ontology,
    )
    stage(
        "hpo",
        ["phenotype_annotations"],
        [("", f"{HPO}/phenotype.hpoa", "phenotype.hpoa")],
        table_parser("phenotype_annotations"),
    )
    stage(
        "hpo",
        ["genes_to_phenotype"],
        [("", f"{HPO}/genes_to_phenotype.txt", "genes_to_phenotype.txt")],
        table_parser("genes_to_phenotype"),
    )
    stage(
        "cpic",
        ["pairs"],
        [("", CPIC_PAIRS_URL, "pair_view.json")],
        parse_cpic_pairs,
    )
    stage(
        "fda",
        ["pgx_biomarkers"],
        [("", FDA_BIOMARKERS_URL, "pgx_biomarkers.xlsx")],
        parse_fda_biomarkers,
    )
    logger.info("Genomics curation sources staged.")


if __name__ == "__main__":
    main()
