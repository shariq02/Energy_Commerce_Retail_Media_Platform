# Source download and staging, all ecosystems in one run
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: a menu of the ecosystems (energy, commerce, genomics) and of their
# sources. For each source chosen, run its stages of sources.yml in order:
# download the files, parse them with the named parser, stage the rows in
# data/staging/<source>/ and delete each downloaded file after its parse. A
# source that is in the release record is staged again only after a yes; a failed
# stage is logged and the menu comes back.

import gzip
import io
import json
import re
import sqlite3
import sys
import tarfile
import zipfile
from datetime import date, timedelta
from fnmatch import fnmatch
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import (
    REQUEST_TIMEOUT,
    clinvar_positions,
    clinvar_variant_codes,
    recorded,
    recorded_tags,
    stage,
    table_parser,
    unpack,
    variant_codes,
)
from _energy_commerce_formats import (
    dwd_files,
    dwd_zip,
    events_csv,
    fetch_bigquery,
    ga4_tables,
    honda_iot,
    mastr_export,
    mastr_xml,
    parse_ga4_events,
    parse_power_plant_list,
    parse_redispatch,
    parse_smard_chart,
    smard_series,
)
from _genomic_formats import (
    BATCH_ROWS,
    GTF_ATTRIBUTE_PATTERN,
    GTF_COLUMNS,
    VCF_INFO_PATTERN,
    add_fields,
    read_parquet,
    read_table,
    read_table_prefix,
    read_uniprot,
    stringify_nested,
)

from config import DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

SOURCES_FILE = Path(__file__).with_name("sources.yml")

HUMAN = "Homo sapiens"
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
    "sequence",
]
UNIPROT_FEATURE_COLUMNS = ["accession", "feature_type", "location", "qualifiers"]
GTF_FEATURES = {"gene", "transcript", "exon", "CDS", "UTR"}
# Silver reads an exon, CDS or UTR row with these columns only; the others stay empty.
GTF_STRUCTURE_FEATURES = {"exon", "CDS", "UTR"}
GTF_STRUCTURE_COLUMNS = ["seqname", "feature", "start", "end", "transcript_id"]
GTF_ATTRIBUTES = [
    "gene_id",
    "gene_name",
    "gene_type",
    "transcript_id",
    "transcript_name",
    "transcript_type",
]
DBVAR_INFO_KEYS = ["SVTYPE", "END", "SVLEN"]
# The ClinGen validity CSV opens with 4 banner lines and a '+++' line under the header.
CLINGEN_VALIDITY_SKIP = [0, 1, 2, 3, 5]
FAERS_DATASETS = ["demo", "drug", "reac", "outc", "rpsr", "ther", "indi"]
OPEN_TARGETS_BATCH_ROWS = 20_000
CADD_QUERY_POSITIONS = 900
# bigWig positions closer than WINDOW_GAP bases are read with one call.
WINDOW_GAP = 1_000
WINDOW_SPAN = 100_000
# The reference genome is staged as 1 Mb chunks with a 50 base flank on each side, so
# that Silver cuts a window of a variant from one chunk. Only the standard contigs.
GENOME_CHUNK_BASES = 1_000_000
GENOME_FLANK = 50
GENOME_BATCH_CHUNKS = 40
GENOME_COLUMNS = ["contig", "chunk_start", "sequence"]
GENOME_CONTIGS = {
    **{f"chr{number}": str(number) for number in range(1, 23)},
    "chrX": "X",
    "chrY": "Y",
    "chrM": "MT",
}
BLOSUM_COLUMNS = ["from_amino_acid", "to_amino_acid", "score"]

# Prediction files hold one 'score' column in the row order of test.parquet.
gpn_scores: dict[str, pd.Series] = {}


# --- row filters of the table parser


def keep_clinvar_variants(frame: pd.DataFrame) -> pd.Series:
    """Rows whose chromosome, position, reference and alternate base are a
    staged ClinVar single-nucleotide variant."""
    codes = variant_codes(frame["CHROM"], frame["POS"], frame["REF"], frame["ALT"])
    wanted = clinvar_variant_codes()
    at = np.minimum(np.searchsorted(wanted, codes), len(wanted) - 1)
    return pd.Series((codes >= 0) & (wanted[at] == codes), index=frame.index)


FILTERS = {
    "assembly_grch37": lambda f: f["Assembly"] == "GRCh37",
    "assembly_grch38": lambda f: f["Assembly"] == "GRCh38",
    "tax_id_human": lambda f: f["tax_id"] == HUMAN_TAX_ID,
    "not_suppressed": lambda f: f["SUPPRESS"] != "Y",
    "reactome_human": lambda f: f["species"] == HUMAN,
    "reactome_human_relation": lambda f: (
        f["parent_pathway_id"].str.startswith("R-HSA")
        & f["child_pathway_id"].str.startswith("R-HSA")
    ),
    "clinvar_variants": keep_clinvar_variants,
    "canonical_or_mane": lambda f: (
        (f["canonical"] == "true") | (f["mane_select"] == "true")
    ),
}


# --- parsers: parse(path, tag) yields ({dataset: frame}, rows read)


def read_text_csv(source, **options) -> pd.DataFrame:
    return pd.read_csv(
        source, dtype=str, keep_default_na=False, na_values=[""], **options
    )


def text_frame(records: list[dict]) -> pd.DataFrame:
    """Records as text; lists and objects are kept as JSON."""
    frame = pd.DataFrame(records)
    for column in frame.columns:
        frame[column] = frame[column].map(
            lambda v: json.dumps(v) if isinstance(v, list | dict) else v
        )
    return frame


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
        frame = add_fields(frame, "attributes", GTF_ATTRIBUTES, GTF_ATTRIBUTE_PATTERN)
        unused = [c for c in frame.columns if c not in GTF_STRUCTURE_COLUMNS]
        frame.loc[frame["feature"].isin(GTF_STRUCTURE_FEATURES), unused] = None
        yield {"annotation": frame}, read


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


def parse_clinvar_vs_benign(path: Path, tag: str):
    frame = pd.concat(read_parquet(path))
    if tag != "test":
        gpn_scores[tag] = frame["score"]
        yield {}, len(frame)
        return
    for model, scores in gpn_scores.items():
        if len(scores) != len(frame):
            raise RuntimeError(
                f"{model}: {len(scores)} scores for {len(frame)} variants"
            )
        frame[model] = scores.to_numpy()
    yield {"variants": frame}, len(frame)


def parse_spectralbio(path: Path, tag: str):
    frame = pd.DataFrame(json.loads(path.read_text(encoding="utf-8")))
    frame.insert(0, "subset", tag)
    yield {"variants": frame}, len(frame)


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


def parse_gnomad(path: Path, tag: str):
    """Read one row group at a time and keep the rows at ClinVar positions."""
    wanted = pa.array(
        clinvar_positions().get(tag.removeprefix("chr"), np.empty(0, np.int32)),
        type=pa.int64(),
    )
    parquet = pq.ParquetFile(path)
    kept: list[pa.Table] = []
    read = 0
    try:
        for group in range(parquet.num_row_groups):
            table = parquet.read_row_group(group)
            kept.append(table.filter(pc.is_in(table.column("pos"), value_set=wanted)))
            read += table.num_rows
            del table
            if read >= BATCH_ROWS:
                yield {"variants": pa.concat_tables(kept).to_pandas()}, read
                kept, read = [], 0
        if read:
            yield {"variants": pa.concat_tables(kept).to_pandas()}, read
    finally:
        parquet.close()


def windows(positions: list[int]) -> list[list[int]]:
    """Group sorted positions into runs that one bigWig call can read."""
    runs: list[list[int]] = []
    for position in positions:
        run = runs[-1] if runs else None
        if run and position - run[-1] <= WINDOW_GAP and position - run[0] < WINDOW_SPAN:
            run.append(position)
        else:
            runs.append([position])
    return runs


def bigwig_frame(rows: list[tuple[str, int, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["chromosome", "position", "score"])


def parse_bigwig(path: Path, tag: str):
    try:
        import pybigtools
    except ImportError as error:
        raise RuntimeError("pybigtools is not installed; install it first") from error

    positions = clinvar_positions()
    reader = pybigtools.open(str(path))
    try:
        sizes = reader.chroms()
        for chromosome, wanted in positions.items():
            name = "chrM" if chromosome == "MT" else f"chr{chromosome}"
            if name not in sizes:
                continue
            rows: list[tuple[str, int, float]] = []
            for run in windows(wanted.tolist()):
                values = reader.values(name, run[0] - 1, run[-1])
                rows += [(chromosome, p, values[p - run[0]]) for p in run]
                if len(rows) >= BATCH_ROWS:
                    yield {tag: bigwig_frame(rows)}, len(rows)
                    rows = []
            if rows:
                yield {tag: bigwig_frame(rows)}, len(rows)
            logger.info(f"{tag}: chromosome {chromosome} done")
    finally:
        close = getattr(reader, "close", None)
        if close:
            close()


CIF_FIELDS = ("label_atom_id", "label_seq_id", "label_comp_id", "B_iso_or_equiv")


def ca_confidence(text: str) -> list[tuple[str, str, str]]:
    """(residue number, residue, pLDDT) of every C-alpha atom of an mmCIF model."""
    names: list[str] = []
    rows: list[tuple[str, str, str]] = []
    at: list[int] = []
    for line in text.splitlines():
        if line.startswith("_atom_site."):
            names.append(line.split()[0].removeprefix("_atom_site."))
        elif names and line.startswith("ATOM"):
            if not at:
                at = [names.index(n) for n in CIF_FIELDS]
            values = line.split()
            if values[at[0]] == "CA":
                rows.append((values[at[1]], values[at[2]], values[at[3]]))
        elif rows:
            break
    return rows


ALPHAFOLD_COLUMNS = ["accession", "fragment", "residue_number", "residue", "plddt"]


def alphafold_frame(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=ALPHAFOLD_COLUMNS)


def parse_alphafold(path: Path, tag: str):
    """Per-residue model confidence (pLDDT, in B_iso_or_equiv) of each model."""
    rows: list[tuple] = []
    models = 0
    with tarfile.open(path, "r|") as archive:
        for member in archive:
            name = Path(member.name).name
            if not name.endswith(".cif.gz"):
                continue
            accession, fragment = name.split("-")[1:3]
            text = gzip.decompress(archive.extractfile(member).read()).decode()
            rows += [(accession, fragment, *row) for row in ca_confidence(text)]
            models += 1
            if len(rows) >= BATCH_ROWS:
                yield {"residue_confidence": alphafold_frame(rows)}, models
                rows, models = [], 0
    if rows or models:
        yield {"residue_confidence": alphafold_frame(rows)}, models


def parse_energy_charts(path: Path, tag: str):
    """One row per quarter-hour and border; unit and licence from the payload."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("data") or []
    frame = text_frame(
        [
            {"timestamp": record["timestamp"], "border": border, "value": value}
            for record in records
            for border, value in record["values"].items()
        ]
    )
    frame["unit"] = payload.get("unit")
    frame["sign_convention"] = payload.get("attributes", {}).get("sign_convention")
    frame["license"] = payload.get("license")
    yield {"physical_flows": frame}, len(records)


def html_table_rows(path: Path, first_header: str | None = None) -> list[list[str]]:
    """Rows of the first HTML table, or of the first table whose first cell reads
    `first_header`; a cell spanning rows is repeated below."""
    from bs4 import BeautifulSoup

    tables = BeautifulSoup(
        path.read_text(encoding="utf-8", errors="replace"), "html.parser"
    ).find_all("table")
    table = next(
        (
            t
            for t in tables
            if first_header is None
            or (t.find(["th", "td"]) or t).get_text(" ", strip=True) == first_header
        ),
        None,
    )
    if table is None:
        raise RuntimeError(f"{path.name}: no table found")
    rows: list[list[str]] = []
    pending: dict[int, list] = {}
    for tr in table.find_all("tr"):
        cells = iter(tr.find_all(["th", "td"]))
        row: list[str] = []
        while True:
            column = len(row)
            if column in pending:
                text, left = pending[column]
                row.append(text)
                if left > 1:
                    pending[column] = [text, left - 1]
                else:
                    del pending[column]
                continue
            cell = next(cells, None)
            if cell is None:
                break
            text = cell.get_text(" ", strip=True)
            row.append(text)
            span = int(cell.get("rowspan", 1))
            if span > 1:
                pending[column] = [text, span - 1]
        if row:
            rows.append(row)
    return rows


def parse_acmg(path: Path, tag: str):
    header, *body = html_table_rows(path)
    width = len(header)
    frame = pd.DataFrame([(r + [None] * width)[:width] for r in body], columns=header)
    yield {"secondary_findings_genes": frame}, len(body)


def parse_tcga_site_codes(path: Path, tag: str):
    """Tissue source site code and study name of the GDC TCGA code table."""
    header, *body = html_table_rows(path, first_header="TSS Code")
    width = len(header)
    frame = pd.DataFrame([(r + [None] * width)[:width] for r in body], columns=header)
    frame = frame.rename(columns={"TSS Code": "tss_code", "Study Name": "study_name"})
    yield {"site_codes": frame[["tss_code", "study_name"]]}, len(body)


def parse_depmap_effect(path: Path, tag: str):
    """The model x gene matrix as rows (model, gene, effect); values unchanged."""
    for frame in read_text_csv(path, chunksize=50):
        model = frame.columns[0]
        long = frame.melt(id_vars=[model], var_name="gene", value_name="gene_effect")
        yield {"gene_effect": long.rename(columns={model: "model_id"})}, len(frame)


def genome_chunk_frames(contig: str, bases: str):
    """Chunk rows of one contig: (contig, 1-based start of the chunk, its bases with a
    GENOME_FLANK of N or neighbour bases on each side), in batches of rows."""
    padded = "N" * GENOME_FLANK + bases + "N" * GENOME_FLANK
    rows = []
    for start in range(0, len(bases), GENOME_CHUNK_BASES):
        sequence = padded[start : start + GENOME_CHUNK_BASES + 2 * GENOME_FLANK]
        rows.append((contig, start + 1, sequence))
        if len(rows) >= GENOME_BATCH_CHUNKS:
            yield pd.DataFrame(rows, columns=GENOME_COLUMNS)
            rows = []
    if rows:
        yield pd.DataFrame(rows, columns=GENOME_COLUMNS)


def parse_genome_chunks(path: Path, tag: str):
    """The standard contigs of a gzip FASTA as chunk rows (upper case)."""
    contig, lines = None, []
    with gzip.open(path, "rt", encoding="ascii") as handle:
        for line in handle:
            if line.startswith(">"):
                if contig:
                    bases = "".join(lines).upper()
                    for frame in genome_chunk_frames(contig, bases):
                        yield {"chunk": frame}, len(bases)
                    logger.info(f"genome: contig {contig} done")
                contig, lines = GENOME_CONTIGS.get(line[1:].split()[0]), []
            elif contig:
                lines.append(line.strip())
    if contig:
        bases = "".join(lines).upper()
        for frame in genome_chunk_frames(contig, bases):
            yield {"chunk": frame}, len(bases)
        logger.info(f"genome: contig {contig} done")


def parse_blosum(path: Path, tag: str):
    """A substitution matrix of the NCBI text format as rows (from, to, score)."""
    lines = path.read_text(encoding="ascii").splitlines()
    rows = [line.split() for line in lines if line.strip() and line[0] != "#"]
    header, *body = rows
    records = [
        (row[0], column, score)
        for row in body
        for column, score in zip(header, row[1:], strict=True)
    ]
    yield {"scores": pd.DataFrame(records, columns=BLOSUM_COLUMNS)}, len(body)


# --- parser factories: the arguments come from the `parser` mapping of a stage


def zipped(members: dict, read=None):
    """Delimited members of a zip, matched by file-name pattern ({tag} filled). A
    member is a pattern, or a mapping with `file` and the `usecols` to keep."""

    def parse(path: Path, tag: str):
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            for dataset, pattern in members.items():
                options = dict(read or {})
                if isinstance(pattern, dict):
                    options["usecols"] = pattern["usecols"]
                    pattern = pattern["file"]
                dataset, pattern = dataset.format(tag=tag), pattern.format(tag=tag)
                member = next(
                    (n for n in names if fnmatch(Path(n).name, pattern)), None
                )
                if member is None:
                    raise RuntimeError(f"{path.name}: no member matches {pattern}")
                size_mb = archive.getinfo(member).file_size / 1024**2
                logger.info(f"{path.name}: extracting {member} ({size_mb:.0f} MB)")
                extracted = Path(archive.extract(member, path.parent))
                try:
                    for frame in read_table(extracted, **options):
                        yield {dataset: frame}, len(frame)
                finally:
                    extracted.unlink()

    return parse


def vcf(dataset, info_keys, keep=None):
    """A VCF: optional row filter, named INFO fields copied into columns."""

    def parse(path: Path, tag: str):
        for frame in read_table(path):
            read = len(frame)
            if keep:
                frame = frame.loc[FILTERS[keep](frame)].copy()
            yield (
                {dataset: add_fields(frame, "INFO", info_keys, VCF_INFO_PATTERN)},
                read,
            )

    return parse


def homologies(species):
    """Ensembl Compara homologies of human: paralogues (both genes human) and
    orthologues to the named species (gene order in a row is arbitrary)."""
    wanted = set(species)

    def parse(path: Path, tag: str):
        for frame in read_table(path):
            first, second = frame["species"], frame["homology_species"]
            kind = frame["homology_type"].fillna("")
            human = (first == "homo_sapiens") | (second == "homo_sapiens")
            paralogue = kind.str.contains("paralog") & (first == second) & human
            orthologue = (
                kind.str.startswith("ortholog")
                & human
                & (first.isin(wanted) | second.isin(wanted))
            )
            batches = {
                "orthologues": frame.loc[orthologue],
                "paralogues": frame.loc[paralogue],
            }
            yield batches, len(frame)

    return parse


def iedb(dataset, columns=None):
    """An IEDB export: two header rows joined, human-host rows kept; `columns`
    keeps a named subset ("Category | Field")."""

    def parse(path: Path, tag: str):
        with zipfile.ZipFile(path) as archive:
            member = next(n for n in archive.namelist() if n.endswith(".csv"))
            with archive.open(member) as raw:
                frames = pd.read_csv(
                    io.TextIOWrapper(raw, encoding="utf-8"),
                    header=[0, 1],
                    dtype=str,
                    keep_default_na=False,
                    na_values=[""],
                    chunksize=BATCH_ROWS,
                )
                for frame in frames:
                    frame.columns = [f"{a} | {b}" for a, b in frame.columns]
                    read = len(frame)
                    host = next(
                        (c for c in frame.columns if c.startswith("Host | Name")), None
                    )
                    if host:
                        frame = frame.loc[
                            frame[host].str.contains("Homo sapiens", na=False)
                        ]
                    if columns:
                        frame = frame.reindex(columns=columns)
                    yield {dataset: frame}, read

    return parse


def table(
    dataset,
    keep=None,
    tag_column=None,
    read=None,
    usecols_in=None,
    rename=None,
    columns=None,
):
    """`rename` maps column names; `columns` keeps a fixed column set (a column a
    file lacks becomes null), for files of one source whose columns differ."""
    options = dict(read or {})
    if usecols_in is not None:
        wanted = set(usecols_in)
        options["usecols"] = lambda column: column in wanted
    parse = table_parser(
        dataset,
        keep=FILTERS[keep] if keep else None,
        tag_column=tag_column,
        **options,
    )
    if not (rename or columns):
        return parse
    fixed = ([tag_column] if tag_column else []) + list(columns or [])

    def shaped(path: Path, tag: str):
        for batches, read_rows in parse(path, tag):
            frame = batches[dataset].rename(columns=rename or {})
            yield (
                {dataset: frame.reindex(columns=fixed) if columns else frame},
                read_rows,
            )

    return shaped


def reference(dataset, keep_human):
    def parse(path: Path, tag: str):
        frame = read_text_csv(path)
        read = len(frame)
        if keep_human:
            frame = frame.loc[frame["taxon"] == "Human"]
        yield {dataset: frame}, read

    return parse


def human_dms_files() -> set[str]:
    chunks = (DATA_STAGING_DIR / "proteingym").glob(
        "proteingym_dms_reference_chunk_*.csv"
    )
    files = {name for chunk in chunks for name in read_text_csv(chunk)["DMS_filename"]}
    if not files:
        raise RuntimeError(
            "Stage proteingym/dms_reference first: no human DMS list found"
        )
    return files


def substitutions(dataset, id_column, columns, human_only):
    def parse(path: Path, tag: str):
        wanted = human_dms_files() if human_only else None
        batch: list[pd.DataFrame] = []
        rows = 0
        with zipfile.ZipFile(path) as archive:
            for member in archive.namelist():
                name = Path(member).name
                if not name.endswith(".csv") or (
                    wanted is not None and name not in wanted
                ):
                    continue
                with archive.open(member) as f:
                    frame = read_text_csv(f, usecols=lambda c: c in columns)
                if id_column not in frame:
                    frame.insert(0, id_column, name.removesuffix(".csv"))
                batch.append(frame)
                rows += len(frame)
                if rows >= BATCH_ROWS:
                    yield {dataset: pd.concat(batch)}, rows
                    batch, rows = [], 0
        if batch:
            yield {dataset: pd.concat(batch)}, rows

    return parse


def parquet(dataset):
    def parse(path: Path, tag: str):
        for frame in read_parquet(path):
            yield {dataset: frame}, len(frame)

    return parse


def csv(dataset, rename=None):
    """A comma-separated file; `rename` maps column names (pandas names an empty
    header `Unnamed: 0`)."""

    def parse(path: Path, tag: str):
        frames = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            chunksize=BATCH_ROWS,
        )
        for frame in frames:
            yield {dataset: frame.rename(columns=rename or {})}, len(frame)

    return parse


def open_targets(dataset, dropped=()):
    # The yearly time series stay as JSON text; Silver reads them into years.
    def parse(path: Path, tag: str):
        for frame in read_parquet(path, chunksize=OPEN_TARGETS_BATCH_ROWS):
            frame = frame.drop(columns=list(dropped), errors="ignore")
            yield {dataset: stringify_nested(frame)}, len(frame)

    return parse


def metadata(dataset):
    def parse(path: Path, tag: str):
        for frame in read_parquet(path):
            yield {dataset: stringify_nested(frame)}, len(frame)

    return parse


PLAIN_PARSERS = {
    "gene2refseq": parse_gene2refseq,
    "dbvar": parse_dbvar,
    "uniprot": parse_uniprot,
    "gtf": parse_gtf,
    "clingen_validity": parse_clingen_validity,
    "hpo_ontology": parse_hpo_ontology,
    "cpic_pairs": parse_cpic_pairs,
    "fda_biomarkers": parse_fda_biomarkers,
    "clinvar_vs_benign": parse_clinvar_vs_benign,
    "spectralbio": parse_spectralbio,
    "faers": parse_faers,
    "maps": parse_maps,
    "cadd": parse_cadd,
    "gnomad": parse_gnomad,
    "bigwig": parse_bigwig,
    "alphafold": parse_alphafold,
    "energy_charts": parse_energy_charts,
    "acmg": parse_acmg,
    "tcga_site_codes": parse_tcga_site_codes,
    "depmap_effect": parse_depmap_effect,
    "genome_chunks": parse_genome_chunks,
    "blosum": parse_blosum,
    "smard_chart": parse_smard_chart,
    "power_plant_list": parse_power_plant_list,
    "redispatch": parse_redispatch,
    "ga4_events": parse_ga4_events,
}
PARSER_FACTORIES = {
    "dwd_zip": dwd_zip,
    "mastr_xml": mastr_xml,
    "events_csv": events_csv,
    "honda_iot": honda_iot,
    "zipped": zipped,
    "vcf": vcf,
    "homologies": homologies,
    "iedb": iedb,
    "table": table,
    "reference": reference,
    "substitutions": substitutions,
    "parquet": parquet,
    "csv": csv,
    "open_targets": open_targets,
    "metadata": metadata,
}


def build_parser(spec):
    """The parse function of a stage: a name, or a mapping with `name` and arguments."""
    if isinstance(spec, str):
        return PLAIN_PARSERS[spec]
    arguments = {key: value for key, value in spec.items() if key != "name"}
    return PARSER_FACTORIES[spec["name"]](**arguments)


# --- the stage list


def load_sources() -> dict:
    return yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))


def fill(item: dict, values: dict) -> tuple[str, str, str]:
    return tuple(str(item[key]).format(**values) for key in ("tag", "url", "file"))


def expand_item(item: dict, variables: dict) -> list[tuple[str, str, str]]:
    if "each" in item:
        return [
            fill(
                item,
                {**variables, **(e if isinstance(e, dict) else {"item": e})},
            )
            for e in item["each"]
        ]
    if "range" in item:
        return [fill(item, {**variables, "n": n}) for n in range(item["range"])]
    return [fill(item, variables)]


def open_targets_parts(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    base = variables["opentargets"]
    page = requests.get(f"{base}/{dataset}/", timeout=REQUEST_TIMEOUT).text
    parts = sorted(set(re.findall(r'href="([^"?/][^"]*\.parquet)"', page)))
    if not parts:
        raise RuntimeError(f"opentargets/{dataset}: no Parquet files listed")
    release = str(variables["opentargets_release"])
    return [(release, f"{base}/{dataset}/{part}", part) for part in parts]


def energy_charts_months(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """One API call per calendar month of the window."""
    start = date.fromisoformat(variables["energy_charts_start"])
    end = date.fromisoformat(variables["energy_charts_end"])
    files = []
    month = start.replace(day=1)
    while month <= end:
        following = date(month.year + month.month // 12, month.month % 12 + 1, 1)
        first, last = max(month, start), min(following - timedelta(days=1), end)
        url = f"{variables['energy_charts']}/cbpf?region=de&start={first}&end={last}"
        files.append((f"{month:%Y-%m}", url, f"cbpf_{month:%Y-%m}.json"))
        month = following
    return files


def encode_re2g(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """Released ENCODE-rE2G prediction files of the biosamples whose name holds
    a keyword of `encode_tissue_keywords` (tissues as in GTEx)."""
    base = variables["encode"]
    search = (
        f"{base}/search/?type=Annotation&annotation_type=element+gene+regulatory"
        "+interaction+predictions&status=released&format=json&limit=all"
    )
    headers = {"Accept": "application/json"}
    graph = requests.get(search, timeout=REQUEST_TIMEOUT, headers=headers).json()
    keywords = [k.lower() for k in variables["encode_tissue_keywords"]]
    files = []
    for item in graph["@graph"]:
        tissue = item.get("biosample_ontology", {}).get("term_name", "")
        if "ENCODE-rE2G" not in item.get("description", "") or not any(
            k in tissue.lower() for k in keywords
        ):
            continue
        annotation = requests.get(
            f"{base}{item['@id']}?format=json", timeout=REQUEST_TIMEOUT, headers=headers
        ).json()
        chosen = [
            f
            for f in annotation.get("files", [])
            if isinstance(f, dict)
            and f.get("status") == "released"
            and f.get("file_format") in ("tsv", "bed")
            and "threshold" in f.get("output_type", "")
        ]
        files += [(tissue, f"{base}{f['href']}", Path(f["href"]).name) for f in chosen]
    if not files:
        raise RuntimeError("encode: no ENCODE-rE2G thresholded file found")
    logger.info(f"encode: {len(files)} ENCODE-rE2G files")
    return files


LISTINGS = {
    "opentargets_parts": open_targets_parts,
    "energy_charts_months": energy_charts_months,
    "encode_re2g": encode_re2g,
    "smard_series": smard_series,
    "dwd_files": dwd_files,
    "mastr_export": mastr_export,
    "ga4_tables": ga4_tables,
}
# `fetch` of a stage: how a url that is not a plain file is brought to disk.
FETCHERS = {"bigquery": fetch_bigquery}


def stage_files(entry: dict, variables: dict) -> list[tuple[str, str, str]]:
    files = entry["files"]
    if isinstance(files, dict):
        return LISTINGS[files["from"]](entry["datasets"][0], variables)
    return [file for item in files for file in expand_item(item, variables)]


def needs_staging(entry: dict, files: list[tuple[str, str, str]]) -> bool:
    """False when the stage is recorded; a stage marked `refresh_when_files_added`
    runs again when its list holds a tag the record does not have."""
    key = (entry["source"], "+".join(entry["datasets"]))
    if key not in recorded():
        return True
    if not entry.get("refresh_when_files_added"):
        return False
    return not {tag for tag, _, _ in files} <= recorded_tags(*key)


# --- the menu

ECOSYSTEMS = ("energy", "commerce", "genomics")


def ecosystem_of(entry: dict) -> str:
    return entry.get("ecosystem", "genomics")


def sources_of(config: dict, ecosystem: str) -> list[str]:
    """The sources of one ecosystem, in the order of sources.yml."""
    found: list[str] = []
    for entry in config["stages"]:
        if ecosystem_of(entry) == ecosystem and entry["source"] not in found:
            found.append(entry["source"])
    return found


def is_staged(config: dict, source: str, done: set[tuple[str, str]]) -> bool:
    """True when every stage of the source is in the release record."""
    keys = {
        (e["source"], "+".join(e["datasets"]))
        for e in config["stages"]
        if e["source"] == source
    }
    return keys <= done


def parse_choice(text: str, count: int) -> list[int] | None:
    """The numbers (1 to `count`) of a menu answer: `2`, `1,3` or `all`; None when the
    answer is not valid."""
    answer = text.strip().lower()
    if answer == "all":
        return list(range(1, count + 1))
    try:
        numbers = [int(part) for part in answer.split(",")]
    except ValueError:
        return None
    if not all(1 <= number <= count for number in numbers):
        return None
    return sorted(set(numbers))


def stage_entry(entry: dict, files: list[tuple[str, str, str]]) -> bool:
    """Stage one stage of sources.yml; False when it was not staged."""
    fetch = FETCHERS[entry["fetch"]] if "fetch" in entry else None
    return stage(
        entry["source"],
        entry["datasets"],
        files,
        build_parser(entry["parser"]),
        capped=tuple(entry.get("capped", ())),
        keep_manual=entry.get("keep_manual", False),
        pause_seconds=entry.get("pause_seconds", 0),
        force=True,
        timeout_seconds=entry.get("timeout_seconds", REQUEST_TIMEOUT),
        compress=entry.get("compress", False),
        jsonl=entry.get("jsonl", False),
        fetch=fetch,
    )


def run_sources(
    config: dict, chosen: list[str], ask=input
) -> tuple[list[str], list[str]]:
    """Stage the chosen sources, in the order of sources.yml. A stage that is not in
    the release record (or has a new file tag) runs; a source whose stages are all
    recorded runs again only after a yes. Returns (failed, not staged) stage labels."""
    failed: list[str] = []
    not_staged: list[str] = []
    for source in dict.fromkeys(e["source"] for e in config["stages"]):
        if source not in chosen:
            continue
        listed = []
        for entry in (e for e in config["stages"] if e["source"] == source):
            label = f"{source}/{'+'.join(entry['datasets'])}"
            try:
                logger.info(f"{label}: listing the files")
                files = stage_files(entry, config["vars"])
                logger.info(f"{label}: {len(files)} file(s) listed")
            except Exception:
                logger.exception(f"{label}: listing failed; continuing")
                failed.append(label)
                continue
            listed.append((entry, label, files))
        due = [item for item in listed if needs_staging(item[0], item[2])]
        if listed and not due:
            again = ask(f"{source} is staged. Stage it again? [y/N] ")
            if again.strip().lower() != "y":
                continue
            due = listed
        for entry, label, files in due:
            try:
                if not stage_entry(entry, files):
                    not_staged.append(label)
            except Exception:
                logger.exception(f"{label}: failed; continuing with the next stage")
                failed.append(label)
    return failed, not_staged


def ecosystem_page(config: dict, ecosystem: str, ask=input) -> None:
    sources = sources_of(config, ecosystem)
    done = recorded()
    print(f"\n{ecosystem}")
    for number, source in enumerate(sources, 1):
        state = "staged" if is_staged(config, source, done) else "not staged"
        print(f"  {number}. {source} ({state})")
    answer = ask("Numbers (for example 1,3), all, or Enter to go back: ")
    if not answer.strip():
        return
    picked = parse_choice(answer, len(sources))
    if picked is None:
        print("Not valid: give numbers from the list.")
        return
    failed, not_staged = run_sources(config, [sources[n - 1] for n in picked], ask)
    if failed:
        logger.error(f"Failed: {', '.join(failed)}")
    if not_staged:
        logger.warning(f"Not staged: {', '.join(not_staged)}")
    if not (failed or not_staged):
        logger.info("Staged.")


def main(ask=input) -> None:
    config = load_sources()
    leave = len(ECOSYSTEMS) + 1
    while True:
        print("\nEcosystems")
        for number, name in enumerate(ECOSYSTEMS, 1):
            print(f"  {number}. {name}")
        print(f"  {leave}. exit")
        try:
            answer = ask("Choose a number: ").strip()
        except EOFError:
            return
        if answer == str(leave):
            return
        picked = parse_choice(answer, len(ECOSYSTEMS))
        if picked is None or len(picked) != 1:
            print("Not valid: give one number from the list.")
            continue
        ecosystem_page(config, ECOSYSTEMS[picked[0] - 1], ask)


if __name__ == "__main__":
    main()
