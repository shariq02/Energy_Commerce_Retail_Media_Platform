"""Unit tests for `databricks/silver/_genomics_common.py`, its rules file and the
registry rows built from its structures.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import ast
import copy
import re
from pathlib import Path

import pytest

from src.schemas._generate_field_classes import (
    _genomics_namespace,
    assert_registry_complete,
    build_rows,
)
from tests._notebook_loader import REPO_ROOT, load_genomics_common

pytestmark = [pytest.mark.unit]

_BRONZE_CONFIG = REPO_ROOT / "databricks" / "bronze" / "_genomics_bronze_common.py"
_GOVERNANCE = {
    "source_system",
    "source_record_id",
    "ecosystem",
    "_silver_loaded_at",
    "_silver_run_id",
}
_SPARK_TYPES = re.compile(
    r"^(string|int|bigint|double|boolean|date|timestamp|array<.+>)$"
)


@pytest.fixture(scope="module")
def genomics():
    return load_genomics_common()


@pytest.fixture(scope="module")
def rules(genomics):
    return genomics["load_genomics_rules"]()


def _bronze_sources() -> dict:
    tree = ast.parse(Path(_BRONZE_CONFIG).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "SOURCES" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    message = "SOURCES not found"
    raise AssertionError(message)


# --- declared structures ---------------------------------------------------


def test_the_two_schemas_hold_fifteen_and_twenty_five_tables(genomics):
    reference = genomics["GENOMICS_REFERENCE_TABLES"]
    main = genomics["GENOMICS_MAIN_TABLES"]
    assert len(reference) == 15
    assert len(main) == 25
    assert reference.isdisjoint(main)


def test_every_table_has_a_grain_and_no_stray_table_has_one(genomics):
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    assert set(genomics["GENOMICS_TABLE_GRAIN"]) == tables


def test_target_schema_follows_the_table_sets(genomics):
    assert genomics["genomics_target_schema"]("gene") == "genomics_silver_reference"
    assert genomics["genomics_target_schema"]("variant") == "genomics_silver"
    with pytest.raises(KeyError):
        genomics["genomics_target_schema"]("not_a_genomics_table")


def test_declared_columns_are_lower_snake_case_with_known_types(genomics):
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    for table, columns in genomics["GENOMICS_STRUCTURES"].items():
        assert table in tables, table
        names = [name for name, _ in columns]
        assert len(names) == len(set(names)), f"duplicate column in {table}"
        assert not _GOVERNANCE & set(names), f"{table} repeats a governance column"
        for name, spark_type in columns:
            assert re.fullmatch(r"[a-z][a-z0-9_]*", name), (table, name)
            assert _SPARK_TYPES.match(spark_type), (table, name, spark_type)


# --- the registry the generator builds from them ---------------------------


def test_every_genomics_table_is_classified_in_its_own_schema(genomics):
    ns = _genomics_namespace()
    rows = build_rows()
    tables = ns["GENOMICS_REFERENCE_TABLES"] | ns["GENOMICS_MAIN_TABLES"]
    for table in tables:
        mine = [r for r in rows if r["table_name"] == table]
        columns = {r["column_name"] for r in mine}
        declared = {c for c, _ in ns["GENOMICS_STRUCTURES"].get(table, [])}
        assert columns == declared | _GOVERNANCE, table
        expected = ns["genomics_target_schema"](table)
        assert {r["target_schema"] for r in mine} == {expected}, table


def test_the_registry_places_fifteen_and_twenty_five_genomics_tables():
    rows = build_rows()
    per_schema: dict[str, set[str]] = {}
    for r in rows:
        per_schema.setdefault(r["target_schema"], set()).add(r["table_name"])
    assert len(per_schema["genomics_silver_reference"]) == 15
    assert len(per_schema["genomics_silver"]) == 25


def test_the_registry_is_complete_with_the_genomics_tables():
    assert_registry_complete(build_rows())


# --- rules file ------------------------------------------------------------


def test_the_real_rules_file_is_sound(genomics, rules):
    assert genomics["validate_genomics_rules"](rules, _bronze_sources()) == []


def test_a_missing_section_is_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    del broken["precedence"]
    assert genomics["validate_genomics_rules"](broken, _bronze_sources()) == [
        "missing section 'precedence'"
    ]


def test_an_unknown_source_in_a_precedence_list_is_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    broken["precedence"]["gene"]["symbol"].append("nosuchsource.table.column")
    problems = genomics["validate_genomics_rules"](broken, _bronze_sources())
    assert any("nosuchsource.table.column" in p for p in problems)


def test_review_stars_outside_the_scale_are_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    broken["clinvar_review_stars"]["practice guideline"] = 9
    problems = genomics["validate_genomics_rules"](broken, _bronze_sources())
    assert any("practice guideline" in p for p in problems)


def test_conflicting_classifications_get_one_star(rules):
    stars = rules["clinvar_review_stars"]
    assert stars["criteria provided, conflicting classifications"] == 1
    assert stars["criteria provided, multiple submitters, no conflicts"] == 2


def test_precedence_rank_is_the_one_based_place_in_the_list(genomics, rules):
    rank = genomics["precedence_rank"]
    assert rank(rules, "gene", "symbol", "ncbi_gene.gene_info", "Symbol") == 2
    assert rank(rules, "gene", "hgnc_id", "gencode.annotation", "attributes") == 3
    assert rank(rules, "gene", "symbol", "ncbi_gene.gene_info", "dbXrefs") is None
    assert rank(rules, "no_such_entity", "symbol", "x.y", "z") is None
    assert rank(rules, "coordinate", "symbol", "x.y", "z") is None


def test_the_gene_coordinate_order_lists_gencode_first(rules):
    assert rules["precedence"]["coordinate"][0] == "gencode.annotation"
    assert "tcga.mc3_somatic_mutations" not in rules["precedence"]["coordinate"]


def test_tcga_is_not_a_source_of_the_variant_coordinate(rules):
    order = rules["precedence"]["variant"]["coordinate"]
    assert not any(entry.startswith("tcga.") for entry in order)


# --- parsers ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("chr1", "1"),
        ("chrX", "X"),
        ("chrx", "X"),
        ("M", "MT"),
        ("chrM", "MT"),
        ("MT", "MT"),
        ("22", "22"),
        ("KI270706.1", "KI270706.1"),
        ("chrUn_GL000220v1", "Un_GL000220v1"),
        (" 7 ", "7"),
        ("", None),
        (None, None),
    ],
)
def test_normalise_chromosome(genomics, value, expected):
    assert genomics["normalise_chromosome"](value) == expected


def test_variant_key_joins_the_five_parts(genomics):
    assert genomics["variant_key"]("GRCh38", "chr1", "976215", "a", "g") == (
        "GRCh38:1:976215:A:G"
    )
    assert genomics["variant_key"]("GRCh38", "chrM", 5, "A", "G") == "GRCh38:MT:5:A:G"


@pytest.mark.parametrize(
    "args",
    [
        ("GRCh38", "1", None, "A", "G"),
        ("GRCh38", "1", "12x", "A", "G"),
        ("GRCh38", "1", "100", "", "G"),
        ("GRCh38", None, "100", "A", "G"),
        (None, "1", "100", "A", "G"),
    ],
)
def test_variant_key_is_none_when_a_part_is_missing(genomics, args):
    assert genomics["variant_key"](*args) is None


def test_bed_start_becomes_one_based(genomics):
    assert genomics["bed_to_closed"](0, 10) == (1, 10)


@pytest.mark.parametrize(
    ("accession", "expected"),
    [
        ("ENSG00000135596.20", ("ENSG00000135596", "20")),
        ("NC_000019.10", ("NC_000019", "10")),
        ("ENSG00000135596", ("ENSG00000135596", None)),
        (None, (None, None)),
        ("", (None, None)),
    ],
)
def test_strip_version(genomics, accession, expected):
    assert genomics["strip_version"](accession) == expected


def test_split_values_can_keep_the_empty_positions(genomics):
    text = "a|b||d|"
    assert genomics["split_values"](text) == ["a", "b", "d"]
    assert genomics["split_values"](text, keep_empty=True) == ["a", "b", "", "d", ""]
    assert genomics["split_values"](None) == []


def test_gtf_attributes_keep_a_repeated_key_in_order(genomics):
    text = (
        'gene_id "ENSG00000135596.20"; transcript_id "ENST00000856847.2"; '
        'gene_type "protein_coding"; gene_name "MICAL1"; level 2; '
        'protein_id "ENSP00000526906.1"; hgnc_id "HGNC:20619"; tag "basic"; '
        'tag "TAGENE"; havana_gene "OTTHUMG00000015350.9";'
    )
    found = genomics["parse_gtf_attributes"](text)
    assert found["gene_id"] == ["ENSG00000135596.20"]
    assert found["hgnc_id"] == ["HGNC:20619"]
    assert found["level"] == ["2"]
    assert found["tag"] == ["basic", "TAGENE"]
    assert genomics["parse_gtf_attributes"](None) == {}


def test_gtf_attributes_of_a_refseq_row(genomics):
    text = 'gene_id "TRNP"; transcript_id "rna-TRNP";  gene_name "TRNP";'
    assert genomics["parse_gtf_attributes"](text) == {
        "gene_id": ["TRNP"],
        "transcript_id": ["rna-TRNP"],
        "gene_name": ["TRNP"],
    }


def test_assembly_label_uses_the_rules(genomics, rules):
    label = genomics["assembly_label"]
    assert label("Reference GRCh38.p14 Primary Assembly", rules) == "GRCh38"
    assert label("Alternate T2T-CHM13v2.0", rules) == "T2T-CHM13v2.0"
    assert label("GRCh38.p14 PATCHES", rules) is None
    assert label(None, rules) is None


def test_clinvar_conditions_keep_the_empty_group_and_pair_by_position(genomics):
    rows = genomics["parse_clinvar_conditions"](
        "MONDO:MONDO:0013342,MedGen:C3150901,OMIM:613647,Orphanet:306511"
        "||MedGen:C3661900",
        "Hereditary spastic paraplegia 48|Macular dystrophy with or without "
        "extraocular features|not provided",
        "RCV000000012|RCV005255549|RCV004998069",
    )
    assert [r["position"] for r in rows] == [1, 2, 3]
    assert rows[0]["ids"] == [
        "MONDO:MONDO:0013342",
        "MedGen:C3150901",
        "OMIM:613647",
        "Orphanet:306511",
    ]
    assert rows[1]["ids"] == []
    assert rows[1]["name"] == "Macular dystrophy with or without extraocular features"
    assert rows[2]["accession"] == "RCV004998069"
    assert {r["pairing_status"] for r in rows} == {"paired"}


def test_clinvar_conditions_with_a_trailing_empty_group(genomics):
    rows = genomics["parse_clinvar_conditions"](
        "MedGen:C1|MedGen:C2|MedGen:C3|",
        "a|b|c|FOXRED1-related disorder",
        "RCV1|RCV2|RCV3|RCV4",
    )
    assert len(rows) == 4
    assert rows[3]["ids"] == []
    assert rows[3]["name"] == "FOXRED1-related disorder"


def test_clinvar_conditions_with_unequal_lists_are_marked(genomics):
    rows = genomics["parse_clinvar_conditions"]("MedGen:C1|MedGen:C2", "a", "RCV1|RCV2")
    assert len(rows) == 2
    assert {r["pairing_status"] for r in rows} == {"count_mismatch"}
    assert rows[1]["name"] is None


def test_hgvs_name_delins(genomics):
    out = genomics["parse_hgvs_name"](
        "NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA "
        "(p.Arg27_Ile28delinsLeuLeuTer)"
    )
    assert out["parse_status"] == "ok"
    assert (out["transcript_accession"], out["transcript_version"]) == (
        "NM_014855",
        3,
    )
    assert out["gene_symbol"] == "AP5Z1"
    assert (out["c_start"], out["c_end"], out["c_edit_type"]) == (80, 83, "delins")
    assert out["c_inserted"] == "TGCTGTAAACTGTAACTGTAAA"
    assert (out["p_reference"], out["p_position"]) == ("R", 27)
    assert (out["p_end_reference"], out["p_end_position"]) == ("I", 28)
    assert (out["p_edit_type"], out["p_inserted"]) == ("delins", "LeuLeuTer")


def test_hgvs_name_frameshift_and_deletion(genomics):
    out = genomics["parse_hgvs_name"]("NM_014855.3(AP5Z1):c.1413_1426del (p.Leu473fs)")
    assert out["c_edit_type"] == "deletion"
    assert (out["p_edit_type"], out["p_position"], out["p_alternate"]) == (
        "frameshift",
        473,
        None,
    )


def test_hgvs_name_missense_and_nonsense(genomics):
    missense = genomics["parse_hgvs_name"](
        "NM_014630.3(ZNF592):c.3136G>A (p.Gly1046Arg)"
    )
    assert missense["c_edit_type"] == "substitution"
    assert (missense["c_reference"], missense["c_alternate"]) == ("G", "A")
    assert (missense["p_edit_type"], missense["p_alternate"]) == ("missense", "R")
    nonsense = genomics["parse_hgvs_name"](
        "NM_017547.4(FOXRED1):c.694C>T (p.Gln232Ter)"
    )
    assert (nonsense["p_edit_type"], nonsense["p_alternate"]) == ("nonsense", "*")


def test_hgvs_name_without_a_protein_part_is_ok(genomics):
    out = genomics["parse_hgvs_name"]("NM_000546.6(TP53):c.-30+1G>A")
    assert out["parse_status"] == "ok"
    assert out["p_notation"] is None
    assert out["c_start"] == -30
    assert out["c_start_offset"] == 1
    assert out["c_region"] == "intronic"


def test_hgvs_name_with_an_unreadable_protein_part_is_partial(genomics):
    out = genomics["parse_hgvs_name"]("NM_000001.1(GENE1):c.5A>G (p.?)")
    assert out["parse_status"] == "partial"
    assert out["c_edit_type"] == "substitution"


def test_hgvs_name_of_a_copy_number_variant_is_unparsed(genomics):
    out = genomics["parse_hgvs_name"]("GRCh38/hg38 1p36.33(chr1:1-100)x3")
    assert out["parse_status"] == "unparsed"
    assert out["c_notation"] is None


def test_a_gene_symbol_with_fs_is_not_a_frameshift(genomics):
    out = genomics["parse_hgvs_name"]("NM_000145.4(FSHR):c.1A>G (p.Met1Val)")
    assert out["p_edit_type"] == "missense"


def test_a_gene_symbol_with_ins_is_a_missense(genomics):
    out = genomics["parse_hgvs_name"]("NM_000207.3(INS):c.188G>A (p.Gly63Asp)")
    assert out["p_edit_type"] == "missense"


@pytest.mark.parametrize(
    ("text", "region", "start", "offset"),
    [
        ("c.-71C>A", "five_prime_utr", -71, None),
        ("c.*45G>A", "three_prime_utr", 45, None),
        ("c.123+5G>A", "intronic", 123, 5),
        ("c.123-2A>G", "intronic", 123, -2),
        ("c.100C>T", "coding", 100, None),
    ],
)
def test_hgvs_coding_regions(genomics, text, region, start, offset):
    out = genomics["parse_hgvs_coding"](text)
    assert (out["c_region"], out["c_start"], out["c_start_offset"]) == (
        region,
        start,
        offset,
    )


def test_hgvs_coding_of_a_dot_or_empty_text_is_all_none(genomics):
    assert set(genomics["parse_hgvs_coding"](".").values()) == {None}
    assert set(genomics["parse_hgvs_coding"](None).values()) == {None}


@pytest.mark.parametrize(
    ("text", "edit", "reference", "position", "alternate"),
    [
        ("p.R835W", "missense", "R", 835, "W"),
        ("p.R100*", "nonsense", "R", 100, "*"),
        ("p.Leu10=", "synonymous", "L", 10, "L"),
        ("p.Met1?", "unknown", "M", 1, None),
        ("p.Lys2del", "deletion", "K", 2, None),
        ("p.Lys2_Gly4dup", "duplication", "K", 2, None),
        ("p.Arg123Profs*45", "frameshift", "R", 123, "P"),
        ("p.Ter100Glnext*?", "extension", "*", 100, None),
        ("p.(Arg27Gly)", "missense", "R", 27, "G"),
    ],
)
def test_hgvs_protein_edit_types(genomics, text, edit, reference, position, alternate):
    out = genomics["parse_hgvs_protein"](text)
    assert (
        out["p_edit_type"],
        out["p_reference"],
        out["p_position"],
        out["p_alternate"],
    ) == (edit, reference, position, alternate)


def test_hgvs_protein_that_is_not_notation_is_all_none(genomics):
    assert set(genomics["parse_hgvs_protein"](".").values()) == {None}


def test_one_letter_amino_acid(genomics):
    code = genomics["one_letter_amino_acid"]
    assert (code("Arg"), code("Ter"), code("R"), code("*")) == ("R", "*", "R", "*")
    assert code(None) is None
    assert code("Xyz") is None


def test_tcga_barcode(genomics):
    out = genomics["parse_tcga_barcode"]("TCGA-NH-A5IV-01A-42D-A36X-10")
    assert out["parse_status"] == "ok"
    assert out["participant_barcode"] == "TCGA-NH-A5IV"
    assert out["sample_barcode"] == "TCGA-NH-A5IV-01A"
    assert (out["sample_type_code"], out["vial"]) == ("01", "A")
    assert (out["portion"], out["analyte"]) == ("42", "D")
    assert (out["plate"], out["center"]) == ("A36X", "10")


def test_a_short_tcga_barcode_is_partial_and_a_foreign_one_is_unparsed(genomics):
    short = genomics["parse_tcga_barcode"]("TCGA-NH-A5IV-01A")
    assert short["parse_status"] == "partial"
    assert short["portion"] is None
    assert genomics["parse_tcga_barcode"]("XYZ-1-2-3")["parse_status"] == "unparsed"
    assert genomics["parse_tcga_barcode"](None)["parse_status"] == "unparsed"


def test_bronze_table_name(genomics):
    assert genomics["bronze_table_name"]("ncbi_gene.gene_info") == "ncbi_gene_gene_info"


def test_xref_pairs_keep_later_colons_in_the_value(genomics):
    pairs = genomics["parse_xref_pairs"](
        "MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410|AllianceGenome:HGNC:5"
    )
    assert pairs == [
        ("MIM", "138670"),
        ("HGNC", "HGNC:5"),
        ("Ensembl", "ENSG00000121410"),
        ("AllianceGenome", "HGNC:5"),
    ]


def test_xref_pairs_of_a_comma_list_and_of_bad_input(genomics):
    pairs = genomics["parse_xref_pairs"](
        "Comparative Toxicogenomics Database:1, NCBI Gene:1, no colon, :x, y:",
        ",",
    )
    assert pairs == [("Comparative Toxicogenomics Database", "1"), ("NCBI Gene", "1")]
    assert genomics["parse_xref_pairs"](None) == []


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("NCBI Gene", "ncbi_gene"),
        ("MIM", "omim"),
        ("OMIM", "omim"),
        ("RefSeq RNA", "refseq_rna"),
        ("UniProtKB", "uniprot"),
        ("Alliance-Genome", "alliance_genome"),
        ("", None),
        (None, None),
    ],
)
def test_normalise_id_type_uses_the_rule_aliases(genomics, rules, label, expected):
    assert genomics["normalise_id_type"](label, rules["id_type_aliases"]) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5", "HGNC:5"),
        ("HGNC:5", "HGNC:5"),
        ("HGNC:HGNC:5", "HGNC:5"),
        (" HGNC:20619 ", "HGNC:20619"),
        ("not an id", "not an id"),
        (None, None),
    ],
)
def test_normalise_hgnc(genomics, value, expected):
    assert genomics["normalise_hgnc"](value) == expected


def test_a_json_empty_list_is_a_placeholder(rules):
    assert "[]" in rules["placeholders"]["general"]


def test_the_gene_master_declares_each_field_with_source_and_method(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["gene"]}
    for field in genomics["GENE_FIELDS"]:
        assert {field, f"{field}_source", f"{field}_method"} <= columns
    assert {"gene_key", "key_source", "grch38_contig", "cytoband_arm"} <= columns


def test_every_gene_field_has_a_precedence_list_or_is_unranked(genomics, rules):
    ranked = set(rules["precedence"]["gene"])
    unranked = {"chromosome", "map_location"}
    assert set(genomics["GENE_FIELDS"]) == ranked | unranked


def test_all_forty_tables_have_a_declared_structure(genomics):
    declared = set(genomics["GENOMICS_STRUCTURES"])
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    assert declared == tables


def test_the_variant_master_declares_each_field_with_source_and_method(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["variant"]}
    for field in genomics["VARIANT_FIELDS"]:
        assert {field, f"{field}_source", f"{field}_method"} <= columns
    assert {"variant_key", "key_source", "assembly", "position"} <= columns


def test_every_ranked_variant_field_is_a_declared_variant_field(genomics, rules):
    ranked = set(rules["precedence"]["variant"]) - {"coordinate"}
    assert ranked <= set(genomics["VARIANT_FIELDS"])


def test_the_frequency_bands_rise_and_end_in_common(rules):
    limits = [band["below"] for band in rules["frequency_bands"]]
    assert limits == sorted(limits)
    assert [band["label"] for band in rules["frequency_bands"]] == [
        "ultra_rare",
        "rare",
        "low_frequency",
    ]
    assert rules["frequency_band_default"] == "common"


def test_unordered_band_limits_are_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    broken["frequency_bands"] = [
        {"label": "a", "below": 0.05},
        {"label": "b", "below": 0.01},
    ]
    problems = genomics["validate_genomics_rules"](broken, _bronze_sources())
    assert any("frequency_bands" in problem for problem in problems)


def test_clinvar_writes_minus_one_for_no_dbsnp_id_and_no_gene(rules):
    by_column = rules["placeholders"]["by_column"]
    assert by_column["clinvar.variant_summary.RS_dbSNP"] == ["-1"]
    assert by_column["clinvar.variant_summary.GeneID"] == ["-1"]


def test_alphamissense_genome_names_map_to_assembly_labels(rules):
    assert rules["ucsc_assembly_names"] == {"hg38": "GRCh38", "hg19": "GRCh37"}


def test_the_frequency_bands_keep_the_old_cutoffs(rules):
    assert [(band["label"], band["below"]) for band in rules["frequency_bands"]] == [
        ("ultra_rare", 0.001),
        ("rare", 0.01),
        ("low_frequency", 0.05),
    ]


def test_the_conservation_cutoffs_are_the_old_ones(rules):
    assert rules["conservation"] == {
        "phylop_conserved_above": 2.7,
        "gerp_constrained_above": 4.0,
        "cadd_deleterious_above": 20.0,
        "allele_frequency_common_above": 0.01,
        "allele_frequency_rare_below": 0.001,
    }


def test_the_old_gene_importance_weights_are_in_the_rules(rules):
    importance = rules["gene_importance"]
    assert [(e["from"], e["points"]) for e in importance["disease_count_points"]] == [
        (50, 10),
        (20, 8),
        (10, 5),
        (5, 3),
        (1, 2),
    ]
    assert importance["function_keyword_points"] == 2
    assert importance["cancer_points"] == 3
    assert [e["label"] for e in rules["gene_annotation_quality"]] == [
        "Excellent",
        "Good",
        "Moderate",
    ]


def test_a_rising_from_list_is_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    broken["review_quality_tiers"] = [
        {"from": 1, "label": "Low Quality"},
        {"from": 3, "label": "High Quality"},
    ]
    problems = genomics["validate_genomics_rules"](broken, _bronze_sources())
    assert any("review_quality_tiers" in problem for problem in problems)


def test_an_unknown_flag_in_the_gene_category_rules_is_reported(genomics, rules):
    broken = copy.deepcopy(rules)
    broken["primary_gene_category"][0]["any"].append({"flag": "has_no_such_flag"})
    problems = genomics["validate_genomics_rules"](broken, _bronze_sources())
    assert any("has_no_such_flag" in problem for problem in problems)


def test_declared_column_names_use_no_shorthand_words(genomics):
    shorthand = {
        "idx",
        "dt",
        "seq",
        "seqs",
        "mfr",
        "rept",
        "init",
        "occp",
        "vbm",
        "tpm",
        "msa",
        "dms",
        "pdb",
        "lfc",
        "rcv",
        "scv",
        "pmid",
        "tss",
        "trt",
        "ctrl",
        "num",
        "cod",
        "cum",
        "nda",
        "wt",
        "grp",
    }
    for table, columns in genomics["GENOMICS_STRUCTURES"].items():
        for name, _ in columns:
            assert not shorthand & set(name.split("_")), (table, name)
