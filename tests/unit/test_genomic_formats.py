"""Unit tests for scripts/ingestion/_genomic_formats.py -- the readers of the
genomics source files.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import gzip
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.ingestion._genomic_formats import (
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

pytestmark = [pytest.mark.unit]


def _gz(path, text):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(text)
    return path


def _plain(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def test_hash_header_and_trailing_separator_with_text_kept(tmp_path):
    path = _gz(
        tmp_path / "MGCONSO.RRF.gz",
        "#CUI|SAB|STR|SUPPRESS|\nC1|HPO|Acute abdomen|N|\nC2||NA|Y|\n",
    )
    frame = pd.concat(read_table(path, sep="|"))

    assert list(frame.columns) == ["CUI", "SAB", "STR", "SUPPRESS"]
    assert frame.loc[1, "STR"] == "NA"  # text stays text
    assert pd.isna(frame.loc[1, "SAB"])  # an empty field becomes null


def test_comment_lines_before_the_header_are_skipped(tmp_path):
    path = _plain(
        tmp_path / "variation_allele.txt",
        "##Mapping report\n"
        "#VariationID:   the identifier\n"
        "#VariationID\tType\tAlleleID\n"
        "2\tVariant\t15041\n",
    )
    frame = pd.concat(read_table(path))

    assert list(frame.columns) == ["VariationID", "Type", "AlleleID"]
    assert frame["AlleleID"].tolist() == ["15041"]


def test_skip_lines_finds_the_gct_header(tmp_path):
    path = _plain(
        tmp_path / "median.gct",
        "#1.2\n1\t1\nName\tDescription\tLiver\nENSG1\tA1BG\t0.5\n",
    )
    frame = pd.concat(read_table(path, skip_lines=2))

    assert list(frame.columns) == ["Name", "Description", "Liver"]
    assert frame["Liver"].tolist() == ["0.5"]


def test_gtf_rows_and_attributes(tmp_path):
    path = _gz(
        tmp_path / "annotation.gtf.gz",
        "##format: gtf\n"
        'chr1\tHAVANA\tgene\t11121\t24894\t.\t+\t.\tgene_id "ENSG1"; gene_name "DDX11L16"; level 2;\n'
        'chr1\tHAVANA\ttranscript\t11426\t14409\t.\t+\t.\tgene_id "ENSG1"; transcript_id "ENST1";  gene_name "DDX11L16";\n',
    )
    frame = pd.concat(read_table(path, names=GTF_COLUMNS))
    frame = add_fields(
        frame,
        "attributes",
        ["gene_id", "transcript_id", "gene_name"],
        GTF_ATTRIBUTE_PATTERN,
    )

    assert frame["feature"].tolist() == ["gene", "transcript"]
    assert frame["transcript_id"].isna().tolist() == [True, False]
    assert frame["gene_name"].tolist() == ["DDX11L16", "DDX11L16"]


def test_vcf_header_and_info_fields(tmp_path):
    path = _gz(
        tmp_path / "study.vcf.gz",
        "##fileformat=VCFv4.1\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "1\t10001\tnssv1\tT\t<DEL>\t.\t.\tSVTYPE=DEL;IMPRECISE;CIEND=0,.;END=5532775;SVLEN=-5522775\n",
    )
    frame = pd.concat(read_table(path))
    frame = add_fields(frame, "INFO", ["SVTYPE", "END", "SVLEN"], VCF_INFO_PATTERN)

    assert frame.columns[0] == "CHROM"
    assert frame.loc[0, ["SVTYPE", "END", "SVLEN"]].tolist() == [
        "DEL",
        "5532775",
        "-5522775",
    ]


def test_prefix_reader_keeps_only_matching_lines(tmp_path):
    path = _gz(
        tmp_path / "gene2refseq.gz",
        "#tax_id\tGeneID\n23\t1\n9606\t2\n9606\t3\n",
    )
    batches = list(read_table_prefix(path, "9606\t"))
    frame = pd.concat(batch for batch, _ in batches)

    assert frame["GeneID"].tolist() == ["2", "3"]
    assert sum(scanned for _, scanned in batches) == 3


def test_parquet_is_read_in_batches(tmp_path):
    path = tmp_path / "test.parquet"
    pq.write_table(pa.table({"chrom": ["1", "2", "3"], "pos": [10, 20, 30]}), path)
    frames = list(read_parquet(path, chunksize=2))

    assert [len(frame) for frame in frames] == [2, 1]
    assert pd.concat(frames)["pos"].tolist() == [10, 20, 30]


def test_uniprot_keeps_human_entries_and_listed_features(tmp_path):
    lines = [
        "ID   P53_HUMAN               Reviewed;         393 AA.",
        "AC   P04637; Q15086;",
        "DE   RecName: Full=Cellular tumor antigen p53 {ECO:0000305};",
        "GN   Name=TP53 {ECO:0000312}; Synonyms=P53;",
        "OX   NCBI_TaxID=9606;",
        f"FT   {'DOMAIN':<16}94..292",
        "FT" + " " * 19 + '/note="DNA-binding"',
        f"FT   {'CHAIN':<16}1..393",
        "SQ   SEQUENCE   393 AA;",
        "//",
        "ID   P53_MOUSE               Reviewed;         390 AA.",
        "AC   P02340;",
        "OX   NCBI_TaxID=10090;",
        "//",
    ]
    path = _gz(tmp_path / "uniprot_sprot.dat.gz", "\n".join(lines) + "\n")
    batches = list(read_uniprot(path, "9606", {"DOMAIN"}))
    entries = [entry for batch in batches for entry in batch[0]]
    features = [feature for batch in batches for feature in batch[1]]

    assert entries == [
        {
            "entry_name": "P53_HUMAN",
            "sequence_length": "393",
            "accession": "P04637",
            "protein_name": "Cellular tumor antigen p53",
            "gene_name": "TP53",
            "tax_id": "9606",
        }
    ]
    assert features == [
        {
            "accession": "P04637",
            "feature_type": "DOMAIN",
            "location": "94..292",
            "qualifiers": '/note="DNA-binding"',
        }
    ]
    assert sum(batch[2] for batch in batches) == 2


def test_nested_parquet_values_become_text_and_scalars_stay(tmp_path):
    path = tmp_path / "nested.parquet"
    pq.write_table(
        pa.table(
            {
                "id": ["a", "b"],
                "score": [1.5, None],
                "idx": [[1, 2], None],
                "detail": [{"x": 1, "y": [3]}, None],
                "blob": [b"\x00\x01", None],
            }
        ),
        path,
    )
    frame = stringify_nested(pd.concat(read_parquet(path)))

    assert frame.loc[0, "idx"] == "[1, 2]"
    assert json.loads(frame.loc[0, "detail"]) == {"x": 1, "y": [3]}
    assert frame.loc[0, "blob"] == "AAE="
    assert frame.loc[1, "idx"] is None
    assert frame.loc[0, "score"] == 1.5
    assert list(frame["id"]) == ["a", "b"]
