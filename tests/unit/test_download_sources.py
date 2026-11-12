"""Unit tests for scripts/download/download_sources.py and sources.yml -- the
genomics stages must cover the genomics Bronze sources and every stage must
expand without a network.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
import zipfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_DIR = ROOT / "scripts" / "download"
BRONZE_CONFIG = ROOT / "databricks" / "bronze" / "_genomics_bronze_common.py"


def _bronze_sources() -> dict[str, list[str]]:
    tree = ast.parse(BRONZE_CONFIG.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "SOURCES" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    message = "SOURCES not found in the genomics Bronze config"
    raise AssertionError(message)


@pytest.fixture(scope="module")
def genomics():
    sys.path.insert(0, str(DOWNLOAD_DIR))
    try:
        spec = importlib.util.spec_from_file_location(
            "download_sources", DOWNLOAD_DIR / "download_sources.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(DOWNLOAD_DIR))
    return module


@pytest.fixture(scope="module")
def config(genomics):
    return genomics.load_sources()


def _files(genomics, config, source, dataset):
    entry = next(
        e
        for e in config["stages"]
        if e["source"] == source and e["datasets"] == [dataset]
    )
    return genomics.stage_files(entry, config["vars"])


def test_stages_cover_every_bronze_dataset(config):
    staged: dict[str, set[str]] = {}
    for entry in config["stages"]:
        if entry.get("ecosystem", "genomics") != "genomics":
            continue
        staged.setdefault(entry["source"], set()).update(entry["datasets"])
    expected = {s: set(d) for s, d in _bronze_sources().items()}
    assert staged == expected


def test_a_stage_key_is_used_once(config):
    keys = [(e["source"], "+".join(e["datasets"])) for e in config["stages"]]
    assert len(keys) == len(set(keys))


def test_capped_datasets_belong_to_their_stage(config):
    for entry in config["stages"]:
        assert set(entry.get("capped", [])) <= set(entry["datasets"])


def test_every_parser_is_built(genomics, config):
    for entry in config["stages"]:
        assert callable(genomics.build_parser(entry["parser"])), entry["source"]


def test_unknown_parser_name_fails(genomics):
    with pytest.raises(KeyError):
        genomics.build_parser("no_such_parser")


def test_every_stage_expands_to_files(genomics, config, monkeypatch):
    monkeypatch.setattr(
        genomics,
        "open_targets_parts",
        lambda dataset, variables: [("26.09", f"https://x/{dataset}/a.parquet", "a")],
    )
    monkeypatch.setitem(
        genomics.LISTINGS, "opentargets_parts", genomics.open_targets_parts
    )
    monkeypatch.setitem(
        genomics.LISTINGS,
        "encode_re2g",
        lambda dataset, variables: [("liver", "https://x/a.tsv.gz", "a.tsv.gz")],
    )
    for entry in config["stages"]:
        files = genomics.stage_files(entry, config["vars"])
        label = f"{entry['source']}/{entry['datasets']}"
        assert files, label
        names = [name for _, _, name in files]
        assert len(names) == len(set(names)), label
        for _, url, name in files:
            assert not re.search(r"[{}]", url + name), label
            assert url.startswith(("https://", "manual:https://")), label


@pytest.mark.parametrize(
    ("source", "dataset", "count"),
    [
        ("clinvar", "variant_summary", 9),
        ("ddinter", "interactions", 8),
        ("dbvar", "variants", 3),
        ("reactome", "pathway_mappings", 2),
        ("clinvar_vs_benign", "variants", 4),
        ("spectralbio", "variants", 3),
        ("maps_embeddings", "metadata", 9),
        ("gnomad", "variants", 24),
        ("tahoe", "pseudobulk_differential_expression", 1026),
    ],
)
def test_file_counts(genomics, config, source, dataset, count):
    assert len(_files(genomics, config, source, dataset)) == count


def test_snapshot_tags_and_order(genomics, config):
    files = _files(genomics, config, "clinvar", "variant_summary")
    assert [tag for tag, _, _ in files] == [
        "current",
        "2026-01",
        "2025-01",
        "2024-01",
        "2023-01",
        "2022-01",
        "2021-01",
        "2020-01",
        "2019-01",
    ]
    assert files[-1][1].endswith("/archive/2019/variant_summary_2019-01.txt.gz")


def test_benchmark_models_come_before_the_test_file(genomics, config):
    files = _files(genomics, config, "clinvar_vs_benign", "variants")
    assert [tag for tag, _, _ in files][-1] == "test"
    assert files[0][1].endswith("/predictions/GPN-Star-V100.parquet")


def test_urls_fill_their_variables(genomics, config):
    links = _files(genomics, config, "string", "protein_links")[0]
    assert links == (
        "",
        (
            "https://stringdb-downloads.org/download/protein.links.detailed.v12.5/"
            "9606.protein.links.detailed.v12.5.txt.gz"
        ),
        "9606.protein.links.detailed.v12.5.txt.gz",
    )
    gencode = _files(genomics, config, "gencode", "annotation")[0]
    assert gencode[2] == "gencode.v50.basic.annotation.gtf.gz"
    last = _files(genomics, config, "tahoe", "pseudobulk_differential_expression")[-1]
    assert last[1].endswith("train-01025-of-01026.parquet")
    assert last[2] == "pseudobulk_differential_expression_01025.parquet"
    chembl = _files(genomics, config, "chembl", "compounds")[0]
    assert chembl[0] == "37"


def test_manual_file_keeps_its_prefix(genomics, config):
    fda = _files(genomics, config, "fda", "pgx_biomarkers")[0]
    assert fda[1].startswith("manual:https://www.fda.gov/")


def test_filters_named_in_the_stages_exist(genomics, config):
    for entry in config["stages"]:
        keep = (
            entry["parser"].get("keep") if isinstance(entry["parser"], dict) else None
        )
        assert keep is None or keep in genomics.FILTERS


def test_open_targets_listing_reads_the_parquet_links(genomics, config, monkeypatch):
    class Page:
        text = (
            '<a href="part-1.parquet">1</a><a href="?C=N">sort</a>'
            '<a href="part-0.parquet">0</a><a href="part-1.parquet">1</a>'
        )

    monkeypatch.setattr(genomics.requests, "get", lambda url, timeout: Page())
    parts = genomics.open_targets_parts("target", config["vars"])
    assert [name for _, _, name in parts] == ["part-0.parquet", "part-1.parquet"]
    assert parts[0][0] == "26.09"
    assert parts[0][1].endswith("/output/target/part-0.parquet")


def test_windows_group_close_positions(genomics):
    assert genomics.windows([1, 2, 500, 5000, 5001]) == [[1, 2, 500], [5000, 5001]]


def test_table_keeps_a_fixed_column_set(genomics, tmp_path):
    table = tmp_path / "e.tsv"
    table.write_text(
        "#chr\tstart\textra\tTargetGeneEnsembl_ID\n1\t5\tx\tENSG1\n", encoding="utf-8"
    )
    parse = genomics.table(
        "links",
        tag_column="biosample",
        rename={"TargetGeneEnsembl_ID": "TargetGeneEnsemblID"},
        columns=["chr", "start", "TargetGeneEnsemblID", "Score"],
    )
    batches, _ = next(parse(table, "liver"))
    frame = batches["links"]
    assert list(frame.columns) == [
        "biosample",
        "chr",
        "start",
        "TargetGeneEnsemblID",
        "Score",
    ]
    assert frame.iloc[0]["TargetGeneEnsemblID"] == "ENSG1"
    assert frame["Score"].isna().all()


def test_iedb_keeps_human_rows_and_named_columns(genomics, tmp_path):
    archive = tmp_path / "i.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr(
            "i.csv",
            (
                "Assay ID,Host,Host,Assay\n"
                "IEDB IRI,Name,IRI,Units\n"
                "a1,Homo sapiens (human),h1,nM\n"
                "a2,Mus musculus,m1,nM\n"
            ),
        )
    parse = genomics.iedb("ligands", columns=["Assay ID | IEDB IRI", "Host | Name"])
    batches, read = next(parse(archive, "v3"))
    frame = batches["ligands"]
    assert read == 2
    assert list(frame.columns) == ["Assay ID | IEDB IRI", "Host | Name"]
    assert frame["Assay ID | IEDB IRI"].tolist() == ["a1"]


def test_homologies_split_paralogues_and_orthologues(genomics, tmp_path):
    table = tmp_path / "h.tsv"
    table.write_text(
        (
            "gene_stable_id\tspecies\thomology_type\thomology_species\n"
            "G1\thomo_sapiens\twithin_species_paralog\thomo_sapiens\n"
            "G1\thomo_sapiens\tortholog_one2one\tmus_musculus\n"
            "G1\thomo_sapiens\tortholog_one2one\tpan_troglodytes\n"
        ),
        encoding="utf-8",
    )
    parse = genomics.homologies(["mus_musculus"])
    batches, read = next(parse(table, "116"))
    assert read == 3
    assert batches["paralogues"]["homology_species"].tolist() == ["homo_sapiens"]
    assert batches["orthologues"]["homology_species"].tolist() == ["mus_musculus"]


def test_energy_charts_months_cover_the_window(genomics, config):
    months = genomics.energy_charts_months("physical_flows", config["vars"])
    assert months[0][1].endswith("start=2024-11-01&end=2024-11-30")
    assert months[-1][1].endswith("start=2026-11-01&end=2026-11-12")
    assert len(months) == 25


def test_needs_staging_refreshes_only_marked_stages(genomics, monkeypatch):
    monkeypatch.setattr(genomics, "recorded", lambda: {("clinvar", "variant_summary")})
    monkeypatch.setattr(genomics, "recorded_tags", lambda s, d: {"current"})
    entry = {"source": "clinvar", "datasets": ["variant_summary"]}
    files = [("current", "u", "f"), ("2019-01", "u", "g")]
    assert not genomics.needs_staging(entry, files)
    assert genomics.needs_staging({**entry, "refresh_when_files_added": True}, files)
    assert not genomics.needs_staging(
        {**entry, "refresh_when_files_added": True}, files[:1]
    )


def test_html_table_rows_repeat_a_spanned_cell(genomics, tmp_path):
    page = tmp_path / "t.html"
    page.write_text(
        (
            "<table><tr><th>Gene</th><th>Condition</th></tr>"
            '<tr><td>BRCA1</td><td rowspan="2">HBOC</td></tr>'
            "<tr><td>BRCA2</td></tr></table>"
        ),
        encoding="utf-8",
    )
    assert genomics.html_table_rows(page) == [
        ["Gene", "Condition"],
        ["BRCA1", "HBOC"],
        ["BRCA2", "HBOC"],
    ]


def test_mmcif_confidence_reads_c_alpha_rows(genomics):
    text = (
        "loop_\n"
        "_atom_site.group_PDB\n"
        "_atom_site.label_atom_id\n"
        "_atom_site.label_comp_id\n"
        "_atom_site.label_seq_id\n"
        "_atom_site.B_iso_or_equiv\n"
        "ATOM N MET 1 50.1\n"
        "ATOM CA MET 1 51.2\n"
        "ATOM CA ALA 2 88.0\n"
        "#\n"
    )
    assert genomics.ca_confidence(text) == [("1", "MET", "51.2"), ("2", "ALA", "88.0")]
