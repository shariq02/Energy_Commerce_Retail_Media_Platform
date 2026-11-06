# Genomics sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: run every stage of genomics_sources.yml in order: download the files,
# parse them with the named parser, stage the rows in data/staging/<source>/ and
# delete each downloaded file after its parse. A stage already in the release
# record is skipped.

import gzip
import json
import re
import sqlite3
import sys
import zipfile
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
    stage,
    table_parser,
    unpack,
    variant_codes,
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

SOURCES_FILE = Path(__file__).with_name("genomics_sources.yml")

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
DBVAR_INFO_KEYS = ["SVTYPE", "END", "SVLEN"]
# The ClinGen validity CSV opens with 4 banner lines and a '+++' line under the header.
CLINGEN_VALIDITY_SKIP = [0, 1, 2, 3, 5]
FAERS_DATASETS = ["demo", "drug", "reac", "outc", "rpsr", "ther", "indi"]
OPEN_TARGETS_BATCH_ROWS = 20_000
CADD_QUERY_POSITIONS = 900
# bigWig positions closer than WINDOW_GAP bases are read with one call.
WINDOW_GAP = 1_000
WINDOW_SPAN = 100_000

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
    "assembly_grch38": lambda f: f["Assembly"] == "GRCh38",
    "tax_id_human": lambda f: f["tax_id"] == HUMAN_TAX_ID,
    "not_suppressed": lambda f: f["SUPPRESS"] != "Y",
    "reactome_human": lambda f: f["species"] == HUMAN,
    "reactome_human_relation": lambda f: (
        f["parent_pathway_id"].str.startswith("R-HSA")
        & f["child_pathway_id"].str.startswith("R-HSA")
    ),
    "clinvar_variants": keep_clinvar_variants,
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
        yield (
            {
                "annotation": add_fields(
                    frame, "attributes", GTF_ATTRIBUTES, GTF_ATTRIBUTE_PATTERN
                )
            },
            read,
        )


def parse_pharmgkb(path: Path, tag: str):
    member = f"{tag}.tsv"
    with zipfile.ZipFile(path) as archive:
        extracted = Path(archive.extract(member, path.parent))
    try:
        for frame in read_table(extracted):
            yield {tag: frame}, len(frame)
    finally:
        extracted.unlink()


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


def parse_biogrid(path: Path, tag: str):
    with zipfile.ZipFile(path) as archive:
        member = next(n for n in archive.namelist() if "Homo_sapiens" in n)
        extracted = Path(archive.extract(member, path.parent))
    try:
        for frame in read_table(extracted):
            yield {"interactions": frame}, len(frame)
    finally:
        extracted.unlink()


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


# --- parser factories: the arguments come from the `parser` mapping of a stage


def table(dataset, keep=None, tag_column=None, read=None, usecols_in=None):
    options = dict(read or {})
    if usecols_in is not None:
        wanted = set(usecols_in)
        options["usecols"] = lambda column: column in wanted
    return table_parser(
        dataset,
        keep=FILTERS[keep] if keep else None,
        tag_column=tag_column,
        **options,
    )


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


def csv(dataset):
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


def open_targets(dataset, dropped=()):
    # Nested time series make the association rows very large; Silver never reads them.
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
    "pharmgkb": parse_pharmgkb,
    "clingen_validity": parse_clingen_validity,
    "hpo_ontology": parse_hpo_ontology,
    "cpic_pairs": parse_cpic_pairs,
    "fda_biomarkers": parse_fda_biomarkers,
    "biogrid": parse_biogrid,
    "clinvar_vs_benign": parse_clinvar_vs_benign,
    "spectralbio": parse_spectralbio,
    "faers": parse_faers,
    "maps": parse_maps,
    "cadd": parse_cadd,
    "gnomad": parse_gnomad,
    "bigwig": parse_bigwig,
}
PARSER_FACTORIES = {
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


LISTINGS = {"opentargets_parts": open_targets_parts}


def stage_files(entry: dict, variables: dict) -> list[tuple[str, str, str]]:
    files = entry["files"]
    if isinstance(files, dict):
        return LISTINGS[files["from"]](entry["datasets"][0], variables)
    return [file for item in files for file in expand_item(item, variables)]


def main() -> None:
    config = load_sources()
    for entry in config["stages"]:
        source, datasets = entry["source"], entry["datasets"]
        if (source, "+".join(datasets)) in recorded():
            logger.info(f"{source}/{'+'.join(datasets)}: already staged, skipping")
            continue
        stage(
            source,
            datasets,
            stage_files(entry, config["vars"]),
            build_parser(entry["parser"]),
            capped=tuple(entry.get("capped", ())),
        )
    logger.info("Genomics sources staged.")


if __name__ == "__main__":
    main()
