"""Unit tests for scripts/ingestion/_chunk_writer.py -- the physical
chunk-rotation and column-alignment logic every staging script relies on.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from scripts.ingestion._chunk_writer import ChunkedCSVWriter, ChunkedJSONLWriter

pytestmark = [pytest.mark.unit]


def test_csv_writer_writes_header_once_across_multiple_write_calls(tmp_path):
    writer = ChunkedCSVWriter(tmp_path, source="test", dataset="rows", max_rows=1000)
    writer.write(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    writer.write(pd.DataFrame({"a": [3], "b": ["z"]}))
    writer.close()

    assert len(writer.chunk_paths) == 1
    content = writer.chunk_paths[0].read_text(encoding="utf-8")
    assert content.count("a,b") == 1  # header appears once, not once per write()
    assert writer.total_rows == 3


def test_csv_writer_rotates_to_a_new_chunk_at_the_row_limit(tmp_path):
    writer = ChunkedCSVWriter(tmp_path, source="test", dataset="rows", max_rows=2)
    writer.write(pd.DataFrame({"a": [1, 2, 3, 4, 5]}))
    writer.close()

    assert len(writer.chunk_paths) == 3  # 2 + 2 + 1 rows across 3 chunks
    assert writer.total_rows == 5
    for path in writer.chunk_paths:
        assert path.exists()


def test_csv_writer_locks_column_order_on_first_batch_and_rejects_drift(tmp_path):
    writer = ChunkedCSVWriter(tmp_path, source="test", dataset="rows", max_rows=1000)
    writer.write(pd.DataFrame({"a": [1], "b": [2]}))
    with pytest.raises(RuntimeError, match="absent from the locked schema"):
        writer.write(pd.DataFrame({"a": [1], "c": [2]}))
    writer.close()


def test_csv_writer_with_explicit_columns_reindexes_a_partial_batch(tmp_path):
    writer = ChunkedCSVWriter(
        tmp_path, source="test", dataset="rows", max_rows=1000, columns=["a", "b", "c"]
    )
    # MaStR-style batch: only the fields this particular record happened to have
    writer.write(pd.DataFrame({"a": [1], "c": [3]}))
    writer.close()

    content = writer.chunk_paths[0].read_text(encoding="utf-8")
    assert content.splitlines()[0] == "a,b,c"


def test_csv_writer_unknown_column_with_explicit_columns_raises(tmp_path):
    writer = ChunkedCSVWriter(
        tmp_path, source="test", dataset="rows", max_rows=1000, columns=["a", "b"]
    )
    with pytest.raises(RuntimeError, match="absent from the locked schema"):
        writer.write(pd.DataFrame({"a": [1], "unexpected": [2]}))


def test_jsonl_writer_writes_one_record_per_line(tmp_path):
    writer = ChunkedJSONLWriter(
        tmp_path, source="test", dataset="events", max_rows=1000
    )
    writer.write([{"event": "view_item", "id": 1}, {"event": "purchase", "id": 2}])
    writer.close()

    assert len(writer.chunk_paths) == 1
    lines = writer.chunk_paths[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"event": "view_item", "id": 1}
    assert json.loads(lines[1]) == {"event": "purchase", "id": 2}


def test_jsonl_writer_rotates_to_a_new_chunk_at_the_row_limit(tmp_path):
    writer = ChunkedJSONLWriter(tmp_path, source="test", dataset="events", max_rows=2)
    writer.write([{"id": i} for i in range(5)])
    writer.close()

    assert len(writer.chunk_paths) == 3
    total_lines = sum(
        len(p.read_text(encoding="utf-8").splitlines()) for p in writer.chunk_paths
    )
    assert total_lines == 5
