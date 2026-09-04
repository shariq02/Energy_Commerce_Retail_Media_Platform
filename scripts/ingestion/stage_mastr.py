# MaStR staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: consolidate the MaStR Gesamtdatenexport (a ZIP of per-object-
# type XML, each object type split into numbered shards) in
# data/raw/mastr/ into one logical staging dataset per object type,
# written to data/staging/mastr/. Read directly from the ZIP via
# streaming XML parse -- never extracted or loaded whole into memory
# (uncompressed, the in-scope object types alone run tens of GB).
# Local file operations only -- does not upload anywhere. data/raw/ is
# read-only throughout.
#
# Deferred this pass, not staged: EinheitenSolar, AnlagenEegSolar,
# EinheitenStromSpeicher, AnlagenEegSpeicher, AnlagenStromSpeicher.
# These four solar/storage tables use the identical schema and
# relationship pattern (unit -> market actor, unit -> location/grid
# connection, unit -> EEG record) already established and staged by
# every other generation-technology table below -- deferring them adds
# no new relationship type, only additional row-count depth within a
# pattern already proven. They are ~41 GB of the ~59 GB in-scope total
# and are a follow-up staging pass, not a scope change (still MaStR
# source-family scope) and not silently dropped -- logged explicitly.
#
# Not staged at all (out of source-family scope): gas-side objects and
# large electricity/gas consumer units.

import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_MASTR_DIR = DATA_RAW_DIR / "mastr"
STAGING_MASTR_DIR = DATA_STAGING_DIR / "mastr"

BATCH_SIZE = 50_000
MAX_CHUNK_BYTES = 150 * 1024 * 1024

# Object-type prefix -> output dataset name. One dataset per prefix,
# spanning all of that prefix's numbered shards (MaStR's own physical
# chunking; chunk count is not dataset count, same rule as this
# project's own ChunkedCSVWriter).
# Real entity/relationship data -> staged under analytical/.
ANALYTICAL_PREFIXES = {
    "EinheitenWind": "einheiten_wind",
    "EinheitenBiomasse": "einheiten_biomasse",
    "EinheitenWasser": "einheiten_wasser",
    "EinheitenVerbrennung": "einheiten_verbrennung",
    "EinheitenKernkraft": "einheiten_kernkraft",
    "EinheitenGeothermieGrubengasDruckentspannung": "einheiten_geothermie_gsgk",
    "AnlagenEegWind": "anlagen_eeg_wind",
    "AnlagenEegBiomasse": "anlagen_eeg_biomasse",
    "AnlagenEegWasser": "anlagen_eeg_wasser",
    "AnlagenEegGeothermieGrubengasDruckentspannung": "anlagen_eeg_geothermie_gsgk",
    "AnlagenKwk": "anlagen_kwk",
    "Marktakteure": "marktakteure",
    "MarktakteureUndRollen": "marktakteure_und_rollen",
    "Netzanschlusspunkte": "netzanschlusspunkte",
    "Netze": "netze",
    "Lokationen": "lokationen",
    "Bilanzierungsgebiete": "bilanzierungsgebiete",
    "EinheitenGenehmigung": "einheiten_genehmigung",
    "GeloeschteUndDeaktivierteEinheiten": "geloeschte_deaktivierte_einheiten",
    "GeloeschteUndDeaktivierteMarktakteure": "geloeschte_deaktivierte_marktakteure",
    "EinheitenAenderungNetzbetreiberzuordnungen": "einheiten_aenderung_netzbetreiberzuordnungen",
    "Ertuechtigungen": "ertuechtigungen",
}

# Small code-list/lookup tables -> staged under reference/, same split
# stage_dwd.py (analytical/metadata) and iPinYou (analytical/reference)
# already use.
REFERENCE_PREFIXES = {
    "Einheitentypen": "einheitentypen",
    "Katalogkategorien": "katalogkategorien",
    "Katalogwerte": "katalogwerte",
    "Lokationstypen": "lokationstypen",
    "Marktfunktionen": "marktfunktionen",
    "Marktrollen": "marktrollen",
}

DEFERRED_PREFIXES = [
    "EinheitenSolar",
    "AnlagenEegSolar",
    "EinheitenStromSpeicher",
    "AnlagenEegSpeicher",
    "AnlagenStromSpeicher",
]


def _shard_names(zf: zipfile.ZipFile, prefix: str) -> list[str]:
    return sorted(
        n
        for n in zf.namelist()
        if n == f"{prefix}.xml" or n.startswith(f"{prefix}_") and n.endswith(".xml")
    )


def _iter_records(zf: zipfile.ZipFile, shard_name: str):
    """Stream one XML shard: every direct child of the root element is one
    record (flat tag->text). Root is cleared after each record so the tree
    never accumulates more than one record's worth of memory, regardless
    of file size."""
    with zf.open(shard_name) as f:
        depth = 0
        root = None
        for event, elem in ET.iterparse(f, events=("start", "end")):
            if event == "start":
                depth += 1
                if depth == 1:
                    root = elem
            else:
                if depth == 2:
                    yield {child.tag: child.text for child in elem}
                depth -= 1
                if depth == 0 and root is not None:
                    root.clear()


def stage_object_type(
    prefix: str, dataset_name: str, subfolder: str, monitor: PeakRSSMonitor
) -> tuple[int, list[Path]]:
    out_dir = STAGING_MASTR_DIR / subfolder
    writer = ChunkedCSVWriter(
        out_dir, source="mastr", dataset=dataset_name, max_bytes=MAX_CHUNK_BYTES
    )

    zip_files = list(RAW_MASTR_DIR.glob("Gesamtdatenexport_*.zip"))
    if not zip_files:
        raise FileNotFoundError(f"No MaStR export ZIP found under {RAW_MASTR_DIR}")

    batch: list[dict] = []

    with zipfile.ZipFile(zip_files[0]) as zf:
        shards = _shard_names(zf, prefix)
        if not shards:
            logger.warning(
                f"MaStR {prefix}: no shard files found in export -- skipping"
            )
            return 0, []

        for shard_name in shards:
            for row in _iter_records(zf, shard_name):
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    writer.write(pd.DataFrame(batch))
                    batch = []
                    monitor.check()

    if batch:
        writer.write(pd.DataFrame(batch))

    writer.close()
    logger.info(
        f"Staged {subfolder}/{dataset_name} -- {writer.total_rows} rows, "
        f"{len(writer.chunk_paths)} chunk file(s), from {len(shards)} shard(s)"
    )
    return writer.total_rows, writer.chunk_paths


def main() -> None:
    monitor = PeakRSSMonitor()
    results = {}

    for prefix, dataset_name in ANALYTICAL_PREFIXES.items():
        rows, paths = stage_object_type(prefix, dataset_name, "analytical", monitor)
        results[dataset_name] = (rows, paths)

    for prefix, dataset_name in REFERENCE_PREFIXES.items():
        rows, paths = stage_object_type(prefix, dataset_name, "reference", monitor)
        results[dataset_name] = (rows, paths)

    logger.info("MaStR staging complete.")
    for name, (rows, path) in results.items():
        logger.info(f"  {name}: {rows} rows -> {path}")
    logger.warning(
        f"Deferred this pass (not staged, not scope-excluded): {', '.join(DEFERRED_PREFIXES)}"
    )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )


if __name__ == "__main__":
    main()
