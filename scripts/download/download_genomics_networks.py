# Genomics pathway and interaction sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download Reactome, STRING and BioGRID, keep the human rows and
# stage them in data/staging/<source>/; each downloaded file is deleted after
# its parse.

import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage, table_parser
from _genomic_formats import read_table

from config import get_logger

logger = get_logger(__name__)

REACTOME = "https://reactome.org/download/current"
STRING = "https://stringdb-downloads.org/download"
STRING_VERSION = "v12.5"
BIOGRID_URL = (
    "https://downloads.thebiogrid.org/Download/BioGRID/Latest-Release/"
    "BIOGRID-ORGANISM-LATEST.tab3.zip"
)

HUMAN = "Homo sapiens"
# Reactome files have no header line.
REACTOME_MAPPING_COLUMNS = [
    "identifier",
    "pathway_id",
    "url",
    "pathway_name",
    "evidence",
    "species",
]
REACTOME_PATHWAY_COLUMNS = ["pathway_id", "pathway_name", "species"]
REACTOME_RELATION_COLUMNS = ["parent_pathway_id", "child_pathway_id"]


def is_human(frame):
    return frame["species"] == HUMAN


def is_human_relation(frame):
    return frame["parent_pathway_id"].str.startswith("R-HSA") & frame[
        "child_pathway_id"
    ].str.startswith("R-HSA")


def string_file(name: str) -> tuple[str, str, str]:
    file = f"9606.protein.{name}.{STRING_VERSION}.txt.gz"
    return "", f"{STRING}/protein.{name}.{STRING_VERSION}/{file}", file


def parse_biogrid(path: Path, tag: str):
    with zipfile.ZipFile(path) as archive:
        member = next(n for n in archive.namelist() if "Homo_sapiens" in n)
        extracted = Path(archive.extract(member, path.parent))
    try:
        for frame in read_table(extracted):
            yield {"interactions": frame}, len(frame)
    finally:
        extracted.unlink()


def main() -> None:
    stage(
        "reactome",
        ["pathway_mappings"],
        [
            (id_type, f"{REACTOME}/{name}", name)
            for id_type, name in [
                ("ncbi_gene", "NCBI2Reactome_All_Levels.txt"),
                ("uniprot", "UniProt2Reactome_All_Levels.txt"),
            ]
        ],
        table_parser(
            "pathway_mappings",
            keep=is_human,
            tag_column="id_type",
            names=REACTOME_MAPPING_COLUMNS,
        ),
    )
    stage(
        "reactome",
        ["pathways"],
        [("", f"{REACTOME}/ReactomePathways.txt", "ReactomePathways.txt")],
        table_parser("pathways", keep=is_human, names=REACTOME_PATHWAY_COLUMNS),
    )
    stage(
        "reactome",
        ["pathway_relations"],
        [
            (
                "",
                f"{REACTOME}/ReactomePathwaysRelation.txt",
                "ReactomePathwaysRelation.txt",
            )
        ],
        table_parser(
            "pathway_relations",
            keep=is_human_relation,
            names=REACTOME_RELATION_COLUMNS,
        ),
    )
    stage(
        "string",
        ["protein_links"],
        [string_file("links.detailed")],
        table_parser("protein_links", sep=" "),
    )
    stage(
        "string", ["protein_info"], [string_file("info")], table_parser("protein_info")
    )
    stage(
        "string",
        ["protein_aliases"],
        [string_file("aliases")],
        table_parser("protein_aliases"),
    )
    stage(
        "biogrid",
        ["interactions"],
        [("", BIOGRID_URL, "BIOGRID-ORGANISM-LATEST.tab3.zip")],
        parse_biogrid,
    )
    logger.info("Genomics pathway and interaction sources staged.")


if __name__ == "__main__":
    main()
