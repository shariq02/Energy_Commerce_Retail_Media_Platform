# Genomics benchmark sources: download and staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: download ProteinGym (clinical and human DMS substitutions), the
# ClinVar missense benchmark with its GPN-Star scores, the gnomAD balanced set
# and the BRCA1/TP53 variant sets, and stage them in data/staging/<source>/;
# each downloaded file is deleted after its parse.

import json
import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from _download_common import stage
from _genomic_formats import BATCH_ROWS, read_parquet

from config import DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

PROTEINGYM = "https://marks.hms.harvard.edu/proteingym/ProteinGym_v1.3"
PROTEINGYM_REFERENCE = (
    "https://raw.githubusercontent.com/OATML-Markslab/ProteinGym/main/reference_files"
)
HF = "https://huggingface.co/datasets"
CLINVAR_VS_BENIGN = f"{HF}/songlab/clinvar_vs_benign/resolve/main"
GNOMAD_BALANCED = f"{HF}/songlab/gnomad_balanced/resolve/main"
SPECTRALBIO = f"{HF}/DaviBonetto/spectralbio-clinvar/resolve/main/benchmarks"

GPN_STAR_MODELS = ["GPN-Star-V100", "GPN-Star-P243", "GPN-Star-M447"]
SPECTRALBIO_SUBSETS = {
    "brca1_full_filtered": "brca1/brca1_full_filtered_v1.json",
    "brca1_transfer100": "brca1/brca1_transfer100_v1.json",
    "tp53_canonical": "tp53/tp53_canonical_v1.json",
}

# Prediction files hold one 'score' column in the row order of test.parquet.
gpn_scores: dict[str, pd.Series] = {}


def read_text_csv(source, **options) -> pd.DataFrame:
    return pd.read_csv(
        source, dtype=str, keep_default_na=False, na_values=[""], **options
    )


def parse_reference(dataset: str, keep_human: bool):
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


def parse_substitutions(
    dataset: str, id_column: str, columns: list[str], human_only: bool
):
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


def parse_parquet(dataset: str):
    def parse(path: Path, tag: str):
        for frame in read_parquet(path):
            yield {dataset: frame}, len(frame)

    return parse


def parse_spectralbio(path: Path, tag: str):
    frame = pd.DataFrame(json.loads(path.read_text(encoding="utf-8")))
    frame.insert(0, "subset", tag)
    yield {"variants": frame}, len(frame)


def main() -> None:
    stage(
        "proteingym",
        ["dms_reference"],
        [
            (
                "",
                f"{PROTEINGYM_REFERENCE}/DMS_substitutions.csv",
                "DMS_substitutions.csv",
            )
        ],
        parse_reference("dms_reference", keep_human=True),
    )
    stage(
        "proteingym",
        ["clinical_reference"],
        [
            (
                "",
                f"{PROTEINGYM_REFERENCE}/clinical_substitutions.csv",
                "clinical_substitutions.csv",
            )
        ],
        parse_reference("clinical_reference", keep_human=False),
    )
    stage(
        "proteingym",
        ["dms_substitutions"],
        [
            (
                "",
                f"{PROTEINGYM}/DMS_ProteinGym_substitutions.zip",
                "DMS_ProteinGym_substitutions.zip",
            )
        ],
        parse_substitutions(
            "dms_substitutions",
            "DMS_id",
            ["mutant", "DMS_score", "DMS_score_bin"],
            human_only=True,
        ),
    )
    stage(
        "proteingym",
        ["clinical_substitutions"],
        [
            (
                "",
                f"{PROTEINGYM}/clinical_ProteinGym_substitutions.zip",
                "clinical_ProteinGym_substitutions.zip",
            )
        ],
        parse_substitutions(
            "clinical_substitutions",
            "protein",
            ["protein", "mutant", "DMS_bin_score"],
            human_only=False,
        ),
    )
    stage(
        "clinvar_vs_benign",
        ["variants"],
        [
            (
                model,
                f"{CLINVAR_VS_BENIGN}/predictions/{model}.parquet",
                f"{model}.parquet",
            )
            for model in GPN_STAR_MODELS
        ]
        + [("test", f"{CLINVAR_VS_BENIGN}/test.parquet", "test.parquet")],
        parse_clinvar_vs_benign,
    )
    stage(
        "gnomad_balanced",
        ["variants"],
        [("", f"{GNOMAD_BALANCED}/test.parquet", "test.parquet")],
        parse_parquet("variants"),
    )
    stage(
        "spectralbio",
        ["variants"],
        [
            (subset, f"{SPECTRALBIO}/{file}", Path(file).name)
            for subset, file in SPECTRALBIO_SUBSETS.items()
        ],
        parse_spectralbio,
    )
    logger.info("Genomics benchmark sources staged.")


if __name__ == "__main__":
    main()
