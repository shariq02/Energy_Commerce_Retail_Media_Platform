"""Unit tests for scripts/download/download_sources.py and sources.yml -- the
genomics stages must cover the genomics Bronze sources, every stage must
expand without a network, and the menu must stage the sources that are chosen.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import ast
import gzip
import importlib.util
import re
import sys
import zipfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_DIR = ROOT / "scripts" / "download"
BRONZE_CONFIG = ROOT / "databricks" / "bronze" / "02_load_genomics_bronze.py"


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
    for name in ("smard_series", "dwd_files", "mastr_export"):
        monkeypatch.setitem(
            genomics.LISTINGS,
            name,
            lambda dataset, variables: [("t", "https://x/a.zip", "a.zip")],
        )
    monkeypatch.setitem(
        genomics.LISTINGS,
        "ga4_tables",
        lambda dataset, variables: [("20210131", "bigquery:p.d.t", "t.jsonl")],
    )
    for entry in config["stages"]:
        files = genomics.stage_files(entry, config["vars"])
        label = f"{entry['source']}/{entry['datasets']}"
        assert files, label
        names = [name for _, _, name in files]
        assert len(names) == len(set(names)), label
        for _, url, name in files:
            assert not re.search(r"[{}]", url + name), label
            assert url.startswith(("https://", "manual:", "bigquery:")), label


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


def test_csv_names_the_empty_first_header(genomics, tmp_path):
    table = tmp_path / "ages.csv"
    table.write_text(",Eukaryota,modeAge\nP1,1.0,Eukaryota\n", encoding="utf-8")
    parse = genomics.csv("consensus", rename={"Unnamed: 0": "protein_accession"})
    batches, rows = next(parse(table, "master"))
    assert list(batches["consensus"].columns) == [
        "protein_accession",
        "Eukaryota",
        "modeAge",
    ]
    assert rows == 1


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


def _entry(config, source, dataset):
    return next(
        e
        for e in config["stages"]
        if e["source"] == source and e["datasets"] == [dataset]
    )


def test_faers_lists_ten_quarters_and_refreshes_when_one_is_added(genomics, config):
    entry = next(e for e in config["stages"] if e["source"] == "faers")
    files = genomics.stage_files(entry, config["vars"])
    assert [tag for tag, _, _ in files] == [
        f"{year}q{q}" for year in (2024, 2025) for q in (1, 2, 3, 4)
    ] + ["2026q1", "2026q2"]
    assert entry["refresh_when_files_added"] is True


def test_constraint_keeps_the_canonical_or_mane_transcript_and_its_columns(
    genomics, config, tmp_path
):
    table = tmp_path / "c.tsv"
    table.write_text(
        "gene\tgene_id\ttranscript\tcanonical\tmane_select\tlof.pLI\textra\n"
        "A\t1\tT1\ttrue\ttrue\t0.9\tx\n"
        "A\t1\tT2\tfalse\tfalse\t0.1\tx\n"
        "B\t2\tT3\tNA\ttrue\tNA\tx\n"
        "B\t2\tT4\ttrue\tNA\t0.5\tx\n",
        encoding="utf-8",
    )
    entry = _entry(config, "gnomad_constraint", "metrics")
    parse = genomics.build_parser(entry["parser"])
    batches, read = next(parse(table, "v4.1"))
    frame = batches["metrics"]
    assert list(frame["transcript"]) == ["T1", "T3", "T4"]
    assert list(frame["lof.pLI"]) == ["0.9", "NA", "0.5"]
    assert "extra" not in frame.columns
    assert read == 4


def test_tcga_site_codes_read_the_code_table_not_the_page_navigation(
    genomics, tmp_path
):
    page = tmp_path / "t.html"
    page.write_text(
        "<table><tr><td>Data Portal</td><td>Website</td></tr></table>"
        "<table><thead><tr><th>TSS Code</th><th>Source Site</th><th>Study Name</th>"
        "<th>BCR</th></tr></thead><tbody>"
        "<tr><td>01</td><td>IGC</td><td>Ovarian serous cystadenocarcinoma</td>"
        "<td>IGC</td></tr></tbody></table>",
        encoding="utf-8",
    )
    batches, read = next(genomics.parse_tcga_site_codes(page, ""))
    frame = batches["site_codes"]
    assert list(frame.columns) == ["tss_code", "study_name"]
    assert frame.iloc[0].tolist() == ["01", "Ovarian serous cystadenocarcinoma"]
    assert read == 1


def test_genome_chunks_keep_standard_contigs_with_a_flank_on_each_side(
    genomics, tmp_path, monkeypatch
):
    monkeypatch.setattr(genomics, "GENOME_CHUNK_BASES", 4)
    monkeypatch.setattr(genomics, "GENOME_FLANK", 2)
    path = tmp_path / "g.fa.gz"
    with gzip.open(path, "wt", encoding="ascii") as handle:
        handle.write(">chr1 a\nacgtac\ngt\n>chrUn_x\nAAAA\n>chrM\nTTTT\n")
    frames = [batch["chunk"] for batch, _ in genomics.parse_genome_chunks(path, "")]
    rows = genomics.pd.concat(frames)
    assert rows["contig"].tolist() == ["1", "1", "MT"]
    assert rows["chunk_start"].tolist() == [1, 5, 1]
    assert rows["sequence"].tolist() == ["NNACGTAC", "GTACGTNN", "NNTTTTNN"]


@pytest.mark.parametrize("length", [1, 4, 5, 8, 10, 23])
@pytest.mark.parametrize("width", [1, 3, 7, 50])
def test_streamed_genome_chunks_equal_the_cut_of_the_whole_contig(
    genomics, tmp_path, monkeypatch, length, width
):
    monkeypatch.setattr(genomics, "GENOME_CHUNK_BASES", 5)
    monkeypatch.setattr(genomics, "GENOME_FLANK", 3)
    monkeypatch.setattr(genomics, "GENOME_BATCH_CHUNKS", 2)
    bases = ("acgtn" * 10)[:length]
    lines = [bases[at : at + width] for at in range(0, length, width)]
    path = tmp_path / "g.fa.gz"
    with gzip.open(path, "wt", encoding="ascii") as handle:
        handle.write(">chr2 x\n" + "\n".join(lines) + "\n")
    frames = [batch["chunk"] for batch, _ in genomics.parse_genome_chunks(path, "")]
    padded = "NNN" + bases.upper() + "NNN"
    expected = [("2", at + 1, padded[at : at + 11]) for at in range(0, length, 5)]
    assert list(genomics.pd.concat(frames).itertuples(index=False, name=None)) == (
        expected
    )


def test_a_contig_chunker_holds_about_one_chunk_of_bases(genomics, monkeypatch):
    monkeypatch.setattr(genomics, "GENOME_CHUNK_BASES", 100)
    monkeypatch.setattr(genomics, "GENOME_FLANK", 10)
    chunker = genomics.ContigChunker("1")
    rows = []
    for _ in range(1000):
        rows += chunker.add("ACGTACGTAC\n")
        assert len(chunker.buffer) + chunker.pending <= 100 + 2 * 10 + 10
    rows += chunker.finish()
    assert len(rows) == 100
    assert all(len(sequence) == 120 for _, _, sequence in rows)


def test_genome_and_blosum_stages_are_staged_and_read_by_bronze(config):
    stages = {(s["source"], tuple(s["datasets"])): s for s in config["stages"]}
    assert stages[("genome_sequence", ("chunk",))]["compress"] is True
    assert stages[("blosum62", ("scores",))]["parser"] == "blosum"
    sources = _bronze_sources()
    assert sources["genome_sequence"] == ["chunk"]
    assert sources["blosum62"] == ["scores"]


def test_blosum_text_becomes_rows_and_comments_are_dropped(genomics, tmp_path):
    path = tmp_path / "BLOSUM62"
    path.write_text("# comment\n   A  R\nA  4 -1\nR -1  5\n", encoding="ascii")
    batches, read = next(genomics.parse_blosum(path, ""))
    assert batches["scores"].values.tolist() == [
        ["A", "A", "4"],
        ["A", "R", "-1"],
        ["R", "A", "-1"],
        ["R", "R", "5"],
    ]
    assert read == 2


def test_eurostat_stage_is_a_commerce_csv_with_named_columns(
    genomics, config, tmp_path
):
    entry = _entry(config, "eurostat", "retail_trade")
    assert entry["ecosystem"] == "commerce"
    table = tmp_path / "e.csv"
    table.write_text(
        "DATAFLOW,LAST UPDATE,freq,indic_bt,nace_r2,s_adj,unit,geo,TIME_PERIOD,"
        "OBS_VALUE,OBS_FLAG,CONF_STATUS\n"
        "ESTAT:STS_TRTU_M(1.0),21/11/26 11:00:00,M,VOL_SLS,G47,CA,I21,DE,2024-01,"
        "103.4,p,\n",
        encoding="utf-8",
    )
    batches, _ = next(genomics.build_parser(entry["parser"])(table, "2018-01"))
    assert list(batches["retail_trade"].columns) == [
        "indic_bt",
        "nace_r2",
        "s_adj",
        "unit",
        "geo",
        "TIME_PERIOD",
        "OBS_VALUE",
        "OBS_FLAG",
    ]
    assert batches["retail_trade"].iloc[0]["OBS_VALUE"] == "103.4"


def _small_config():
    def stage(source, ecosystem, dataset):
        return {
            "source": source,
            "ecosystem": ecosystem,
            "datasets": [dataset],
            "files": [{"tag": "t", "url": "https://x/f", "file": "f"}],
            "parser": "csv",
        }

    return {
        "vars": {},
        "stages": [
            stage("a", "energy", "x"),
            stage("b", "energy", "y"),
            stage("c", "commerce", "z"),
        ],
    }


def _run_menu(genomics, monkeypatch, answers, recorded=()):
    config = _small_config()
    staged = []
    monkeypatch.setattr(genomics, "load_sources", lambda: config)
    monkeypatch.setattr(genomics, "recorded", lambda: set(recorded))
    monkeypatch.setattr(genomics, "recorded_tags", lambda source, datasets: {"t"})
    monkeypatch.setattr(
        genomics,
        "stage_entry",
        lambda entry, files: staged.append(entry["source"]) or True,
    )
    prompts = []
    replies = iter(answers)

    def ask(prompt):
        prompts.append(prompt)
        return next(replies)

    genomics.main(ask=ask)
    return staged, prompts


def test_menu_stages_the_chosen_sources_in_file_order_and_comes_back(
    genomics, monkeypatch
):
    staged, prompts = _run_menu(genomics, monkeypatch, ["1", "2,1", "2", "all", "4"])
    assert staged == ["a", "b", "c"]
    assert len(prompts) == 5


def test_menu_rejects_a_number_outside_the_list(genomics, monkeypatch):
    staged, _ = _run_menu(genomics, monkeypatch, ["9", "1", "3", "", "4"])
    assert staged == []


def test_menu_asks_before_a_staged_source_runs_again(genomics, monkeypatch):
    done = [("a", "x")]
    staged, prompts = _run_menu(
        genomics, monkeypatch, ["1", "1", "n", "1", "1", "y", "4"], recorded=done
    )
    assert staged == ["a"]
    assert prompts.count("a is staged. Stage it again? [y/N] ") == 2


@pytest.mark.parametrize(
    ("text", "count", "expected"),
    [
        ("2", 3, [2]),
        ("1,3", 3, [1, 3]),
        ("3,1,3", 3, [1, 3]),
        ("all", 3, [1, 2, 3]),
        ("0", 3, None),
        ("4", 3, None),
        ("x", 3, None),
        ("", 3, None),
        ("1,", 3, None),
    ],
)
def test_parse_choice(genomics, text, count, expected):
    assert genomics.parse_choice(text, count) == expected


def test_every_source_is_in_one_menu_page_and_agrees_with_the_governed_map(
    genomics, config
):
    import yaml

    mapped = {
        m["source_system"]: m["ecosystem"]
        for m in yaml.safe_load(
            (ROOT / "src/schemas/reference/source_ecosystem_map.yml").read_text(
                encoding="utf-8"
            )
        )["mappings"]
    }
    pages = {e: genomics.sources_of(config, e) for e in genomics.ECOSYSTEMS}
    listed = [source for sources in pages.values() for source in sources]
    assert len(listed) == len(set(listed))
    assert set(listed) == {e["source"] for e in config["stages"]}
    for ecosystem, sources in pages.items():
        for source in sources:
            assert mapped[source] == ecosystem, source
    assert {"dwd", "mastr", "smard", "honda_iot"} <= set(pages["energy"])
    assert {"ga4", "rees46", "eurostat"} <= set(pages["commerce"])


def test_stage_entry_passes_the_writer_and_fetch_options(genomics, config, monkeypatch):
    seen = {}
    monkeypatch.setattr(genomics, "stage", lambda *a, **k: seen.update(k) or True)
    entry = _entry(config, "ga4", "events")
    assert genomics.stage_entry(entry, [("t", "bigquery:p.d.t", "t.jsonl")])
    assert seen["jsonl"] is True
    assert seen["fetch"] is genomics.FETCHERS["bigquery"]
    seen.clear()
    genomics.stage_entry(
        next(e for e in config["stages"] if e["source"] == "mastr"), []
    )
    assert seen["compress"] is True
    assert seen["fetch"] is None


def test_stage_writes_json_lines_through_a_fetch_and_deletes_the_raw_file(
    genomics, monkeypatch, tmp_path
):
    common = sys.modules["_download_common"]
    monkeypatch.setattr(common, "DATA_RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(common, "DATA_STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(common, "RELEASE_RECORD", tmp_path / "record.csv")

    def fetch(url, dest, timeout):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("x", encoding="utf-8")
        return {"bytes": 1, "last_modified": ""}

    def parse(path, tag):
        yield {"events": [{"a": 1, "items": [{"b": 2}]}]}, 3

    assert genomics.stage(
        "src",
        ["events"],
        [("20210101", "bigquery:p.d.t", "t.jsonl")],
        parse,
        jsonl=True,
        fetch=fetch,
    )
    chunk = tmp_path / "staging" / "src" / "src_events_chunk_00001.jsonl"
    assert chunk.read_text(encoding="utf-8") == '{"a": 1, "items": [{"b": 2}]}\n'
    assert not (tmp_path / "raw" / "src" / "t.jsonl").exists()
    assert "src,events,20210101" in (tmp_path / "record.csv").read_text(
        encoding="utf-8"
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


def test_gtf_structure_rows_keep_only_the_columns_silver_reads(genomics, tmp_path):
    attributes = 'gene_id "G1"; gene_name "ABC"; transcript_id "T1"; gene_type "x";'
    path = tmp_path / "annotation.gtf"
    path.write_text(
        f"chr1\tHAVANA\tgene\t10\t90\t.\t+\t.\t{attributes}\n"
        f"chr1\tHAVANA\texon\t10\t40\t.\t+\t.\t{attributes}\n",
        encoding="utf-8",
    )
    batches = list(genomics.parse_gtf(path, ""))
    frame = batches[0][0]["annotation"].set_index("feature")

    assert frame.loc["gene", "gene_name"] == "ABC"
    assert frame.loc["gene", "attributes"] == attributes
    assert frame.loc["exon", "transcript_id"] == "T1"
    assert frame.loc["exon", "start"] == "10"
    for column in ("source", "attributes", "gene_id", "gene_name", "gene_type"):
        assert frame[column].isna()["exon"], column


def test_a_zip_member_keeps_only_its_usecols_and_reads_quotes(genomics, tmp_path):
    path = tmp_path / "flat.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "conditions.txt",
            'id|nct_id|name|downcase_name\n1|NCT1|"Heart | Disease"|"heart | disease"\n',
        )
    parse = genomics.zipped(
        {"conditions": {"file": "conditions.txt", "usecols": ["nct_id", "name"]}},
        read={"sep": "|", "quoted": True},
    )
    batches = list(parse(path, ""))
    frame = batches[0][0]["conditions"]

    assert list(frame.columns) == ["nct_id", "name"]
    assert frame.loc[0, "name"] == "Heart | Disease"
