# Shared download-and-stage helper
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: size-checked streamed download, staging into chunked CSV, delete
# of the downloaded file, and the release record, for the scripts that
# download and stage in one step.

import csv
import gc
import shutil
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import closing, suppress
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from functools import cache
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _chunk_writer import ChunkedCSVWriter
from _genomic_formats import read_table
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, LOGS_DIR, get_logger

logger = get_logger(__name__)

MAX_FILE_BYTES = 15 * 1024**3
REQUEST_TIMEOUT = 60
STREAM_CHUNK_BYTES = 1024 * 1024
PROGRESS_SECONDS = 5
RETRIES = 5
RETRY_WAIT_SECONDS = 10
RATE_WAIT_SECONDS = 60
UNPACK_BLOCK_BYTES = 16 * 1024 * 1024
MIN_FREE_BYTES = 5 * 1024**3
STAGE_CAP_BYTES = 1024**3

RELEASE_RECORD = LOGS_DIR / "acquisition" / "release_record.csv"
RECORD_FIELDS = [
    "source",
    "datasets",
    "tag",
    "file",
    "url",
    "last_modified",
    "bytes",
    "rows_read",
    "rows_kept",
    "recorded_at",
]

# A url with this prefix is a page saved by hand to data/raw/<source>/<file name>.
MANUAL = "manual:"

# (tag, url, file name); the tag goes into the discriminator column, if any.
FileSpec = tuple[str, str, str]
# parse(path, tag) yields ({dataset: batch}, rows read).
Parser = Callable[[Path, str], Iterator[tuple[dict[str, pd.DataFrame], int]]]


def _resumable(dest: Path, response: requests.Response) -> bool:
    """True when the server sent the rest of a file that did not change."""
    if response.status_code != 206:
        return False
    stamp = response.headers.get("Last-Modified")
    return not stamp or parsedate_to_datetime(stamp).timestamp() <= dest.stat().st_mtime


def _write(response: requests.Response, dest: Path, have: int, total: int) -> int:
    written = have
    last_log = time.monotonic()
    with open(dest, "ab" if have else "wb") as f:
        for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
            f.write(chunk)
            written += len(chunk)
            if written > MAX_FILE_BYTES:
                break
            if time.monotonic() - last_log > PROGRESS_SECONDS:
                pct = written / total * 100 if total else 0
                logger.info(
                    f"{dest.name}: {written / 1024**2:.0f} of {total / 1024**2:.0f} MB ({pct:.0f}%)"
                )
                last_log = time.monotonic()
    return written


def download(url: str, dest: Path, timeout: float = REQUEST_TIMEOUT) -> dict | None:
    """Stream `url` to `dest`; after a broken connection, continue from the
    bytes on disk; after a 429 answer, wait (Retry-After) and try again. None
    when the file is over MAX_FILE_BYTES."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, RETRIES + 2):
        have = dest.stat().st_size if dest.exists() else 0
        # identity: fda.gov labels a zip as gzip-encoded unless it is asked for
        # an unencoded file.
        headers = {"Accept-Encoding": "identity"}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            with requests.get(
                url, stream=True, timeout=timeout, headers=headers
            ) as response:
                if response.status_code == 429 and attempt <= RETRIES:
                    after = response.headers.get("Retry-After", "")
                    wait = (
                        int(after) if after.isdigit() else RATE_WAIT_SECONDS * attempt
                    )
                    logger.warning(
                        f"{dest.name}: too many requests; waiting {wait} s "
                        f"(retry {attempt} of {RETRIES})"
                    )
                    time.sleep(wait)
                    continue
                if have and response.status_code == 416:
                    logger.info(f"{dest.name}: already complete on disk")
                    return {
                        "bytes": have,
                        "last_modified": response.headers.get("Last-Modified", ""),
                    }
                response.raise_for_status()
                # A compressed transfer: Content-Length is not the file size,
                # and a byte range of it cannot be appended.
                encoded = bool(response.headers.get("Content-Encoding"))
                if have and encoded:
                    dest.unlink()
                    continue
                if have and not _resumable(dest, response):
                    have = 0
                length = (
                    0 if encoded else int(response.headers.get("Content-Length", 0))
                )
                total = have + length if length else 0
                if total > MAX_FILE_BYTES:
                    dest.unlink(missing_ok=True)
                    logger.warning(
                        f"Skipped, {total / 1024**3:.1f} GB is over 15 GB: {url}"
                    )
                    return None
                action = "Resuming" if have else "Downloading"
                logger.info(
                    f"{action} {dest.name} at {have / 1024**2:.0f} of {total / 1024**2:.0f} MB"
                )
                written = _write(response, dest, have, total)
                last_modified = response.headers.get("Last-Modified", "")
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as error:
            if attempt > RETRIES:
                raise
            logger.warning(
                f"{dest.name}: connection broke ({type(error).__name__}); "
                f"retry {attempt} of {RETRIES} from the bytes on disk"
            )
            time.sleep(RETRY_WAIT_SECONDS * attempt)
            continue

        if written > MAX_FILE_BYTES:
            dest.unlink(missing_ok=True)
            logger.warning(f"Stopped, over 15 GB without a size header: {url}")
            return None
        if total and written > total:
            dest.unlink(missing_ok=True)
            raise RuntimeError(f"{dest.name}: expected {total} bytes, got {written}")
        if total and written < total:
            if attempt > RETRIES:
                raise RuntimeError(
                    f"{dest.name}: {written} of {total} bytes after retries"
                )
            logger.warning(f"{dest.name}: ended early; retry {attempt} of {RETRIES}")
            time.sleep(RETRY_WAIT_SECONDS * attempt)
            continue
        return {"bytes": written, "last_modified": last_modified}
    raise RuntimeError(f"{dest.name}: download failed")


def recorded() -> set[tuple[str, str]]:
    if not RELEASE_RECORD.exists():
        return set()
    with open(RELEASE_RECORD, newline="", encoding="utf-8") as f:
        return {(row["source"], row["datasets"]) for row in csv.DictReader(f)}


def recorded_tags(source: str, datasets: str) -> set[str]:
    """The file tags recorded for one stage."""
    if not RELEASE_RECORD.exists():
        return set()
    with open(RELEASE_RECORD, newline="", encoding="utf-8") as f:
        return {
            row["tag"]
            for row in csv.DictReader(f)
            if row["source"] == source and row["datasets"] == datasets
        }


def record(rows: list[dict]) -> None:
    new = not RELEASE_RECORD.exists()
    RELEASE_RECORD.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).isoformat(timespec="seconds")
    with open(RELEASE_RECORD, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RECORD_FIELDS)
        if new:
            writer.writeheader()
        for row in rows:
            writer.writerow({**row, "recorded_at": stamp})


def staged_bytes(writer: ChunkedCSVWriter) -> int:
    return sum(path.stat().st_size for path in writer.chunk_paths)


def stage(
    source: str,
    datasets: list[str],
    files: list[FileSpec],
    parse: Parser,
    capped: tuple[str, ...] = (),
    keep_manual: bool = False,
    pause_seconds: float = 0,
    force: bool = False,
    timeout_seconds: float = REQUEST_TIMEOUT,
) -> None:
    """Download each file, stage its batches, delete it; record when all are done.

    When a dataset in `capped` reaches STAGE_CAP_BYTES, the parse and the
    remaining files stop; list a capped dataset last in `datasets` and in the
    parse order, so no other dataset is cut. `keep_manual` keeps a file saved
    by hand; `pause_seconds` waits between downloads (rate-limited APIs);
    `force` stages even when the stage is recorded (the caller decided);
    `timeout_seconds` is the request timeout (a server that builds its answer
    first needs more)."""
    key = "+".join(datasets)
    if not force and (source, key) in recorded():
        logger.info(f"{source}/{key}: already staged, skipping")
        return

    out_dir = DATA_STAGING_DIR / source
    for dataset in datasets:
        for old in out_dir.glob(f"{source}_{dataset}_chunk_*.csv"):
            old.unlink()
    writers = {d: ChunkedCSVWriter(out_dir, source, d) for d in datasets}
    monitor = PeakRSSMonitor()
    rows = []

    for tag, url, name in files:
        dest = DATA_RAW_DIR / source / name
        if url.startswith(MANUAL):
            if not dest.exists():
                logger.warning(
                    f"{source}/{key}: not staged; save {url.removeprefix(MANUAL)} "
                    f"in a browser as {dest}"
                )
                return
            facts = {"bytes": dest.stat().st_size, "last_modified": ""}
        else:
            facts = download(url, dest, timeout_seconds)
            time.sleep(pause_seconds)
        if facts is None:
            logger.error(f"{source}/{key}: not staged, {name} was skipped")
            return
        read = kept = 0
        cut = False
        with closing(parse(dest, tag)) as parsed:
            for batches, batch_read in parsed:
                for dataset, frame in batches.items():
                    writers[dataset].write(frame)
                    kept += len(frame)
                read += batch_read
                gc.collect()
                pa.default_memory_pool().release_unused()
                rss = monitor.check()
                logger.info(
                    f"{source}/{name}: {read:,} read, {kept:,} kept, "
                    f"{rss / 1024**2:.0f} MB"
                )
                if any(staged_bytes(writers[d]) >= STAGE_CAP_BYTES for d in capped):
                    logger.warning(
                        f"{source}/{key}: reached the {STAGE_CAP_BYTES / 1024**3:.0f} GB "
                        f"cap; stopped after {read:,} rows read"
                    )
                    cut = True
                    break
        if not (keep_manual and url.startswith(MANUAL)):
            dest.unlink()
        rows.append(
            {
                "source": source,
                "datasets": key,
                "tag": tag,
                "file": name,
                "url": url,
                "rows_read": read,
                "rows_kept": kept,
                **facts,
            }
        )
        if cut:
            break

    # Remove the raw folder when it is empty; a folder with files stays.
    with suppress(OSError):
        (DATA_RAW_DIR / source).rmdir()

    for writer in writers.values():
        writer.close()
    record(rows)
    for dataset, writer in writers.items():
        logger.info(
            f"{source}/{dataset}: {writer.total_rows:,} rows in "
            f"{len(writer.chunk_paths)} chunks -> {out_dir}"
        )
    logger.info(f"{source}/{key}: peak memory {monitor.peak_rss_mb:.0f} MB")


def table_parser(
    dataset: str,
    keep: Callable[[pd.DataFrame], pd.Series] | None = None,
    tag_column: str | None = None,
    **read_options,
) -> Parser:
    """Parser for a delimited file: an optional row filter and tag column."""

    def parse(path: Path, tag: str):
        for frame in read_table(path, **read_options):
            read = len(frame)
            if keep is not None:
                frame = frame.loc[keep(frame)].copy()
            if tag_column:
                frame.insert(0, tag_column, tag)
            yield {dataset: frame}, read

    return parse


@cache
def clinvar_positions() -> dict[str, np.ndarray]:
    """Sorted, distinct VCF positions of the staged ClinVar variants, as one
    int32 array per ClinVar chromosome name."""
    chunks = sorted(
        (DATA_STAGING_DIR / "clinvar").glob("clinvar_variant_summary_chunk_*.csv")
    )
    if not chunks:
        raise RuntimeError("Stage clinvar/variant_summary first: no ClinVar positions")
    parts: dict[str, list[np.ndarray]] = {}
    for chunk in chunks:
        frame = pd.read_csv(
            chunk, usecols=["Chromosome", "PositionVCF"], dtype=str
        ).dropna()
        frame = frame[frame["PositionVCF"].str.isdigit()]
        for chromosome, values in frame.groupby("Chromosome")["PositionVCF"]:
            parts.setdefault(chromosome, []).append(
                values.astype("int64").to_numpy(dtype=np.int32)
            )
    positions = {c: np.unique(np.concatenate(v)) for c, v in parts.items()}
    logger.info(
        f"ClinVar positions: {sum(map(len, positions.values())):,} "
        f"in {len(positions)} chromosomes"
    )
    return positions


BASE_CODES = {"A": 0, "C": 1, "G": 2, "T": 3}
CHROMOSOME_CODES = {
    **{str(number): number for number in range(1, 23)},
    "X": 23,
    "Y": 24,
    "MT": 25,
    "M": 25,
}
POSITION_BITS = 30


def variant_codes(
    chromosome: pd.Series, position: pd.Series, ref: pd.Series, alt: pd.Series
) -> np.ndarray:
    """One int64 per row for a single-nucleotide variant, -1 for any other row.

    The code packs chromosome, position, reference and alternate base; a `chr`
    prefix and the letter case of the input are ignored."""
    chromosome_code = (
        chromosome.str.replace(r"^chr", "", case=False, regex=True)
        .str.upper()
        .map(CHROMOSOME_CODES)
    )
    values = (
        chromosome_code.to_numpy(dtype="float64") * 2**POSITION_BITS
        + pd.to_numeric(position, errors="coerce").to_numpy(dtype="float64")
    ) * 16 + (
        ref.str.upper().map(BASE_CODES).to_numpy(dtype="float64") * 4
        + alt.str.upper().map(BASE_CODES).to_numpy(dtype="float64")
    )
    return np.where(np.isnan(values), -1, values).astype(np.int64)


@cache
def clinvar_variant_codes() -> np.ndarray:
    """Sorted, distinct codes of the staged ClinVar single-nucleotide variants."""
    chunks = sorted(
        (DATA_STAGING_DIR / "clinvar").glob("clinvar_variant_summary_chunk_*.csv")
    )
    if not chunks:
        raise RuntimeError("Stage clinvar/variant_summary first: no ClinVar variants")
    parts = []
    for chunk in chunks:
        frame = pd.read_csv(
            chunk,
            usecols=[
                "Chromosome",
                "PositionVCF",
                "ReferenceAlleleVCF",
                "AlternateAlleleVCF",
            ],
            dtype=str,
            keep_default_na=False,
        )
        codes = variant_codes(
            frame["Chromosome"],
            frame["PositionVCF"],
            frame["ReferenceAlleleVCF"],
            frame["AlternateAlleleVCF"],
        )
        parts.append(codes[codes >= 0])
    codes = np.unique(np.concatenate(parts))
    logger.info(f"ClinVar single-nucleotide variants: {len(codes):,}")
    return codes


def unpack(source, target: Path) -> bool:
    """Copy the open file object `source` to `target`, with a progress line
    every GB. False, and `target` deleted, when the disk would run below
    MIN_FREE_BYTES."""
    written = 0
    with open(target, "wb") as out:
        while block := source.read(UNPACK_BLOCK_BYTES):
            out.write(block)
            written += len(block)
            if shutil.disk_usage(target).free < MIN_FREE_BYTES:
                out.close()
                target.unlink(missing_ok=True)
                logger.error(
                    f"{target.name}: disk nearly full after {written / 1024**3:.0f} GB"
                )
                return False
            if written % 1024**3 < len(block):
                logger.info(f"{target.name}: unpacked {written / 1024**3:.0f} GB")
    return True
