# DWD staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: consolidate DWD raw files in data/raw/dwd/ into the logical
# staging datasets (14 analytical measurements + solar + 5 metadata),
# written to data/staging/dwd/ as physical chunk files via
# ChunkedCSVWriter (<=MAX_CHUNK_BYTES each; chunk count is not dataset
# count). Local file operations only -- does not upload anywhere.
# data/raw/ is read-only throughout.
#
# Each analytical measurement's recent/ and historical/ splits are one
# logical dataset, not two -- DWD's own historical cutoff and recent
# start are designed to be contiguous, not overlapping, so no dedup is
# applied across the two.
#
# Memory-safety design: every dataset is written incrementally, chunk
# by chunk (<=CHUNK_SIZE rows), straight to the open ChunkedCSVWriter.
# No frames list + pd.concat() -- a chunk is written and released
# before the next one is read. The two deduped station-level datasets
# keep only a small in-memory set of already-seen row-tuples (not full
# DataFrames) to dedupe while streaming.

import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_DWD_DIR = DATA_RAW_DIR / "dwd"
STAGING_DWD_DIR = DATA_STAGING_DIR / "dwd"
ANALYTICAL_DIR = STAGING_DWD_DIR / "analytical"
METADATA_DIR = STAGING_DWD_DIR / "metadata"

# DWD source files are Latin-1 encoded (German umlauts) -- reading as
# UTF-8 silently corrupts station names such as "Koeln/Bonn".
SOURCE_ENCODING = "latin-1"

CHUNK_SIZE = 100_000
MAX_CHUNK_BYTES = 150 * 1024 * 1024

ANALYTICAL_MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
    "dew_point",
    "soil_temperature",
    "visibility",
    "cloud_type",
    "wind_synop",
    "extreme_wind",
    "weather_phenomena",
]


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    # Trailing ";" in DWD headers produces one fully-empty unnamed column.
    return df.loc[:, ~df.columns.str.match(r"^Unnamed")]


def _iter_semicolon_chunks(path: Path, chunksize: int = CHUNK_SIZE):
    for chunk in pd.read_csv(
        path,
        sep=";",
        encoding=SOURCE_ENCODING,
        skipinitialspace=True,
        chunksize=chunksize,
    ):
        yield _clean_columns(chunk)


def stage_analytical(
    measurement: str, monitor: PeakRSSMonitor
) -> tuple[int, list[str], list[Path]]:
    writer = ChunkedCSVWriter(
        ANALYTICAL_DIR, source="dwd", dataset=measurement, max_bytes=MAX_CHUNK_BYTES
    )
    files_read = 0
    columns: list[str] = []

    for produkt_file in sorted(RAW_DWD_DIR.glob(f"*/{measurement}/*/produkt_*.txt")):
        city = produkt_file.parent.parent.parent.name
        files_read += 1
        for chunk in _iter_semicolon_chunks(produkt_file):
            chunk.insert(1, "city", city)
            if not columns:
                columns = list(chunk.columns)
            writer.write(chunk)
            del chunk
            monitor.check()

    writer.close()
    logger.info(
        f"Staged analytical/{measurement} -- {writer.total_rows} rows, "
        f"{len(writer.chunk_paths)} chunk file(s), {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def stage_solar(monitor: PeakRSSMonitor) -> tuple[int, list[str], list[Path]]:
    """Solar has its own flat layout (station/solar/produkt_*.txt, no
    recent/historical split) and column set -- its own dataset, not folded
    into ANALYTICAL_MEASUREMENTS. Not every station has solar data."""
    writer = ChunkedCSVWriter(
        ANALYTICAL_DIR, source="dwd", dataset="solar", max_bytes=MAX_CHUNK_BYTES
    )
    files_read = 0
    columns: list[str] = []

    for produkt_file in sorted(RAW_DWD_DIR.glob("*/solar/produkt_*.txt")):
        city = produkt_file.parent.parent.name
        files_read += 1
        for chunk in _iter_semicolon_chunks(produkt_file):
            chunk.insert(1, "city", city)
            if not columns:
                columns = list(chunk.columns)
            writer.write(chunk)
            del chunk
            monitor.check()

    writer.close()
    logger.info(
        f"Staged analytical/solar -- {writer.total_rows} rows, "
        f"{len(writer.chunk_paths)} chunk file(s), {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def stage_station_level_metadata(
    filename_prefix: str,
    dataset_name: str,
    monitor: PeakRSSMonitor,
) -> tuple[int, list[str], list[Path]]:
    """Station-level files (Geographie, Stationsname) are duplicated byte-for-byte
    across every measurement folder for a station -- dedupe by content, not by
    dropping any distinct record. Dedup is done against a small set of
    already-written row-tuples (final datasets are tens of rows), never by
    holding the full multi-file DataFrame in memory."""
    writer = ChunkedCSVWriter(
        METADATA_DIR, source="dwd", dataset=dataset_name, max_bytes=MAX_CHUNK_BYTES
    )
    seen: set[tuple] = set()
    files_read = 0
    columns: list[str] = []

    for meta_file in sorted(RAW_DWD_DIR.glob(f"*/*/*/{filename_prefix}_*.txt")):
        files_read += 1
        for chunk in _iter_semicolon_chunks(meta_file):
            if not columns:
                columns = list(chunk.columns)
            # NaN != NaN, so a raw tuple(row) key would never dedupe two
            # identical rows that both carry a NaN (e.g. bis_datum is NaN
            # for a still-active station) -- normalize NaN to None for the
            # dedup key only, the written row keeps its original values.
            # Checked and added to `seen` one row at a time (not built as a
            # separate list first) so a duplicate appearing twice within
            # the same chunk is also caught, not just cross-file/cross-chunk
            # duplicates.
            new_mask = []
            for row in chunk.itertuples(index=False, name=None):
                key = tuple(None if pd.isna(v) else v for v in row)
                is_new = key not in seen
                if is_new:
                    seen.add(key)
                new_mask.append(is_new)
            new_rows = chunk.loc[new_mask]
            if len(new_rows):
                writer.write(new_rows)
            del chunk, new_rows, new_mask
            monitor.check()

    writer.close()
    if writer.total_rows == 0:
        # No rows at all is not expected for this source -- ChunkedCSVWriter
        # writes nothing for an all-empty dataset (by design), so flag this
        # loudly rather than let a missing dataset pass silently.
        logger.warning(
            f"DWD {dataset_name}: zero rows after dedup, no chunk file written"
        )

    logger.info(
        f"Staged metadata/{dataset_name} -- {writer.total_rows} rows after dedup, "
        f"{len(writer.chunk_paths)} chunk file(s), from {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def stage_device_instrument(
    monitor: PeakRSSMonitor,
) -> tuple[int, list[str], list[Path]]:
    """Unique per station per device category, but now duplicated between the
    recent/ and historical/ copies -- deduped by content like the station-level
    files, via a small seen-set (not full DataFrames)."""
    writer = ChunkedCSVWriter(
        METADATA_DIR,
        source="dwd",
        dataset="device_instrument",
        max_bytes=MAX_CHUNK_BYTES,
    )
    seen: set[tuple] = set()
    files_read = 0
    columns: list[str] = []

    for meta_file in sorted(RAW_DWD_DIR.glob("*/*/*/Metadaten_Geraete_*.txt")):
        device_category = meta_file.stem.replace("Metadaten_Geraete_", "")
        device_category = device_category.rsplit("_", 1)[0]  # drop trailing station id
        files_read += 1
        for chunk in _iter_semicolon_chunks(meta_file):
            chunk.insert(1, "device_category", device_category)
            if not columns:
                columns = list(chunk.columns)
            new_mask = []
            for row in chunk.itertuples(index=False, name=None):
                key = tuple(None if pd.isna(v) else v for v in row)
                is_new = key not in seen
                if is_new:
                    seen.add(key)
                new_mask.append(is_new)
            new_rows = chunk.loc[new_mask]
            if len(new_rows):
                writer.write(new_rows)
            del chunk, new_rows, new_mask
            monitor.check()

    writer.close()
    logger.info(
        f"Staged metadata/device_instrument -- {writer.total_rows} rows after dedup, "
        f"{len(writer.chunk_paths)} chunk file(s), {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def stage_parameter_unit(monitor: PeakRSSMonitor) -> tuple[int, list[str], list[Path]]:
    """Unique per station per measurement type, but now duplicated between the
    recent/ and historical/ copies -- deduped by content, same as above."""
    writer = ChunkedCSVWriter(
        METADATA_DIR, source="dwd", dataset="parameter_unit", max_bytes=MAX_CHUNK_BYTES
    )
    seen: set[tuple] = set()
    files_read = 0
    columns: list[str] = []

    for meta_file in sorted(RAW_DWD_DIR.glob("*/*/*/Metadaten_Parameter_*.txt")):
        files_read += 1
        for chunk in _iter_semicolon_chunks(meta_file):
            if not columns:
                columns = list(chunk.columns)
            new_mask = []
            for row in chunk.itertuples(index=False, name=None):
                key = tuple(None if pd.isna(v) else v for v in row)
                is_new = key not in seen
                if is_new:
                    seen.add(key)
                new_mask.append(is_new)
            new_rows = chunk.loc[new_mask]
            if len(new_rows):
                writer.write(new_rows)
            del chunk, new_rows, new_mask
            monitor.check()

    writer.close()
    logger.info(
        f"Staged metadata/parameter_unit -- {writer.total_rows} rows after dedup, "
        f"{len(writer.chunk_paths)} chunk file(s), {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def _iter_fehlwerte_chunks(path: Path, chunksize: int = CHUNK_SIZE):
    """Metadaten_Fehlwerte files end with a 'generiert: <date> -- Deutscher
    Wetterdienst --' footer line that is not a data row and breaks column
    alignment if parsed as CSV -- filtered out before parsing. Files are
    small enough that reading the text and filtering the footer line does
    not violate the streaming budget; the resulting in-memory buffer is
    then handed to pd.read_csv in chunks like every other source, and
    discarded once its chunks are yielded."""
    text = path.read_text(encoding=SOURCE_ENCODING)
    lines = [
        ln
        for ln in text.splitlines()
        if ln.strip() and not ln.lstrip().startswith("generiert:")
    ]
    del text
    buf = io.StringIO("\n".join(lines))
    del lines
    for chunk in pd.read_csv(buf, sep=";", skipinitialspace=True, chunksize=chunksize):
        yield _clean_columns(chunk)
    buf.close()


def stage_missing_value_periods(
    monitor: PeakRSSMonitor,
) -> tuple[int, list[str], list[Path]]:
    """Metadaten_Fehlwerte_<station>_<daterange>.txt holds only the detailed
    missing-value-period rows. Metadaten_Fehldaten_* is a combined report --
    Gesamt_Fehlwerte summary rows followed by the same detailed rows found in
    Metadaten_Fehlwerte_* (confirmed identical via direct inspection) -- so
    Fehldaten is intentionally not read here, to avoid duplicating the detail
    rows or introducing the summary rows as if they were detail records.
    HTML variants are not used."""
    writer = ChunkedCSVWriter(
        METADATA_DIR,
        source="dwd",
        dataset="missing_value_periods",
        max_bytes=MAX_CHUNK_BYTES,
    )
    files_read = 0
    columns: list[str] = []

    for meta_file in sorted(RAW_DWD_DIR.glob("*/*/*/Metadaten_Fehlwerte_*.txt")):
        files_read += 1
        for chunk in _iter_fehlwerte_chunks(meta_file):
            if not columns:
                columns = list(chunk.columns)
            writer.write(chunk)
            del chunk
            monitor.check()

    writer.close()
    logger.info(
        f"Staged metadata/missing_value_periods -- {writer.total_rows} rows, "
        f"{len(writer.chunk_paths)} chunk file(s), {files_read} source files"
    )
    return writer.total_rows, columns, writer.chunk_paths


def main() -> None:
    monitor = PeakRSSMonitor()
    results = {}

    for measurement in ANALYTICAL_MEASUREMENTS:
        rows, columns, paths = stage_analytical(measurement, monitor)
        results[measurement] = (rows, columns, paths)

    rows, columns, paths = stage_solar(monitor)
    results["solar"] = (rows, columns, paths)

    rows, columns, paths = stage_station_level_metadata(
        "Metadaten_Geographie", "station_geography", monitor
    )
    results["station_geography"] = (rows, columns, paths)

    rows, columns, paths = stage_device_instrument(monitor)
    results["device_instrument"] = (rows, columns, paths)

    rows, columns, paths = stage_parameter_unit(monitor)
    results["parameter_unit"] = (rows, columns, paths)

    rows, columns, paths = stage_station_level_metadata(
        "Metadaten_Stationsname_Betreibername",
        "station_name_history",
        monitor,
    )
    results["station_name_history"] = (rows, columns, paths)

    rows, columns, paths = stage_missing_value_periods(monitor)
    results["missing_value_periods"] = (rows, columns, paths)

    logger.info("DWD staging complete.")
    for name, (rows, columns, paths) in results.items():
        logger.info(
            f"  {name}: {rows} rows, {len(columns)} columns, {len(paths)} chunk file(s)"
        )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )


if __name__ == "__main__":
    main()
