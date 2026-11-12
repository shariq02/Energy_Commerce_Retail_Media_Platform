# Genomic file readers
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: read the delimited, GTF, VCF, UniProt flat and Parquet files of
# the genomics sources in batches. Delimited files are read as text; empty
# fields become null; no other value is changed.

import base64
import csv
import gzip
import io
import json
import re
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

BATCH_ROWS = 100_000
SCAN_REPORT_LINES = 5_000_000

GTF_COLUMNS = [
    "seqname",
    "source",
    "feature",
    "start",
    "end",
    "score",
    "strand",
    "frame",
    "attributes",
]
VCF_INFO_PATTERN = r"(?:^|;){key}=([^;]*)"
GTF_ATTRIBUTE_PATTERN = r'(?:^|;)\s*{key} "([^"]*)"'

UNIPROT_TAX = re.compile(r"NCBI_TaxID=(\d+)")
UNIPROT_PROTEIN = re.compile(r"RecName: Full=([^;{]+)")
UNIPROT_GENE = re.compile(r"Name=([^;{]+)")


def open_text(path: Path):
    """Open a plain or gzip text file (gzip found by its magic bytes)."""
    with open(path, "rb") as f:
        gzipped = f.read(2) == b"\x1f\x8b"
    if gzipped:
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def _read_csv(source, sep: str, names: list[str], usecols, **options):
    return pd.read_csv(
        source,
        sep=sep,
        header=None,
        names=names,
        usecols=usecols,
        index_col=False,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        quoting=csv.QUOTE_NONE,
        **options,
    )


def _header(line: str, sep: str) -> list[str]:
    names = [name.strip() for name in line.rstrip("\r\n").lstrip("﻿#").split(sep)]
    while names and not names[-1]:
        names.pop()
    return names


def _data_start(
    path: Path, sep: str, skip_lines: int, names: list[str] | None
) -> tuple[int, list[str]]:
    """Line number of the first data line, and the column names.

    Lines starting with '##' are skipped. Without `names`, the header is the
    first line that holds the separator (a leading '#' is removed)."""
    with open_text(path) as f:
        for number, line in enumerate(f):
            if number < skip_lines or line.startswith("##"):
                continue
            if names is not None and not line.startswith("#"):
                return number, names
            if names is None and sep in line:
                return number + 1, _header(line, sep)
    raise ValueError(f"{path.name}: no header or data line found")


def read_table(
    path: Path,
    sep: str = "\t",
    *,
    skip_lines: int = 0,
    names: list[str] | None = None,
    usecols=None,
    chunksize: int = BATCH_ROWS,
) -> Iterator[pd.DataFrame]:
    """Yield batches of a delimited file."""
    start, columns = _data_start(path, sep, skip_lines, names)
    with open_text(path) as f:
        yield from _read_csv(
            f,
            sep,
            columns,
            usecols if usecols is not None else range(len(columns)),
            skiprows=start,
            chunksize=chunksize,
        )


def read_table_prefix(
    path: Path, prefix: str, sep: str = "\t", chunksize: int = BATCH_ROWS
) -> Iterator[tuple[pd.DataFrame, int]]:
    """Yield (batch, lines scanned) of the lines that start with `prefix`.

    The first line is the header. A batch is also yielded every
    SCAN_REPORT_LINES scanned lines, so progress shows on long files."""
    with open_text(path) as f:
        names = _header(f.readline(), sep)
        lines: list[str] = []
        scanned = 0
        for line in f:
            scanned += 1
            if line.startswith(prefix):
                lines.append(line)
            if len(lines) >= chunksize or scanned >= SCAN_REPORT_LINES:
                yield _lines_frame(lines, names, sep), scanned
                lines, scanned = [], 0
        if lines or scanned:
            yield _lines_frame(lines, names, sep), scanned


def _lines_frame(lines: list[str], names: list[str], sep: str) -> pd.DataFrame:
    if not lines:
        return pd.DataFrame(columns=names, dtype=str)
    return _read_csv(io.StringIO("".join(lines)), sep, names, range(len(names)))


def read_parquet(path: Path, chunksize: int = BATCH_ROWS) -> Iterator[pd.DataFrame]:
    """Yield batches of a Parquet file."""
    for batch in pq.ParquetFile(path).iter_batches(batch_size=chunksize):
        yield batch.to_pandas()


def _json_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"cannot serialise {type(value).__name__}")


def _as_text(value):
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, list | tuple | dict):
        return json.dumps(value, default=_json_default)
    return value


def stringify_nested(frame: pd.DataFrame) -> pd.DataFrame:
    """Lists, structs and arrays become JSON text and bytes become base64
    text, so a Parquet batch can be written as CSV. Other values are kept."""
    for column in frame.columns[frame.dtypes == object]:
        frame[column] = frame[column].map(_as_text)
    return frame


def add_fields(
    frame: pd.DataFrame, column: str, keys: list[str], pattern: str
) -> pd.DataFrame:
    """Copy named fields out of a packed text column (VCF INFO, GTF attributes)."""
    for key in keys:
        regex = pattern.format(key=re.escape(key))
        frame[key] = frame[column].str.extract(regex, expand=False)
    return frame


def read_uniprot(
    path: Path, tax_id: str, feature_types: set[str], chunksize: int = 20_000
) -> Iterator[tuple[list[dict], list[dict], int]]:
    """Yield (entries, features, entries scanned) of a UniProt .dat file.

    Only entries of `tax_id` are kept, and only features of `feature_types`."""
    entries: list[dict] = []
    features: list[dict] = []
    scanned = 0
    entry: dict = {}
    entry_features: list[dict] = []
    with open_text(path) as f:
        for line in f:
            code, body = line[:2], line[5:].rstrip("\n")
            if code == "ID":
                parts = body.split()
                entry = {"entry_name": parts[0], "sequence_length": parts[-2]}
                entry_features = []
            elif code == "AC" and "accession" not in entry:
                entry["accession"] = body.split(";")[0].strip()
            elif code == "DE" and "protein_name" not in entry:
                match = UNIPROT_PROTEIN.search(body)
                if match:
                    entry["protein_name"] = match.group(1).strip()
            elif code == "GN" and "gene_name" not in entry:
                match = UNIPROT_GENE.search(body)
                if match:
                    entry["gene_name"] = match.group(1).strip()
            elif code == "OX":
                match = UNIPROT_TAX.search(body)
                entry["tax_id"] = match.group(1) if match else None
            elif code == "FT" and line[5] != " ":
                entry_features.append(
                    {
                        "feature_type": line[5:21].strip(),
                        "location": line[21:].strip(),
                        "qualifiers": "",
                    }
                )
            elif code == "FT" and entry_features:
                text = line[21:].strip()
                entry_features[-1]["qualifiers"] = (
                    f"{entry_features[-1]['qualifiers']} {text}".strip()
                )
            elif code == "//":
                scanned += 1
                if entry.get("tax_id") == tax_id:
                    entries.append(entry)
                    features.extend(
                        {"accession": entry["accession"], **feature}
                        for feature in entry_features
                        if feature["feature_type"] in feature_types
                    )
                if len(entries) >= chunksize or scanned >= 50_000:
                    yield entries, features, scanned
                    entries, features, scanned = [], [], 0
    if entries or scanned:
        yield entries, features, scanned
