# gnomAD genomes: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download the gnomAD v4.1.1 genomes allele frequencies per
# chromosome, keep the rows at the ClinVar positions, and stage them in
# data/staging/gnomad/; each downloaded file is deleted after its parse.

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import clinvar_positions, stage
from _genomic_formats import BATCH_ROWS

from config import get_logger

logger = get_logger(__name__)

GNOMAD = (
    "https://huggingface.co/datasets/altar-genomics/gnomad-4.1.1-genomes/resolve/main"
)
GNOMAD_CHROMOSOMES = [f"chr{n}" for n in [*range(1, 23), "X", "Y"]]


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


def main() -> None:
    stage(
        "gnomad",
        ["variants"],
        [
            (
                chromosome,
                f"{GNOMAD}/chr={chromosome}/gnomad.genomes.v4.1.1.sites.{chromosome}.parquet",
                f"gnomad.genomes.v4.1.1.sites.{chromosome}.parquet",
            )
            for chromosome in GNOMAD_CHROMOSOMES
        ],
        parse_gnomad,
        capped=("variants",),
    )
    logger.info("gnomAD staged.")


if __name__ == "__main__":
    main()
