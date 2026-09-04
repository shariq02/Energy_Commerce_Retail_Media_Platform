"""CDC -- the registered event schemas are well-formed and cover every table.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import jsonschema
import pytest

from src.ingestion.cdc import config
from src.ingestion.cdc.landing import META_COLUMNS
from src.ingestion.cdc.register_schemas import event_schema

pytestmark = [pytest.mark.cdc, pytest.mark.unit, pytest.mark.schema]


@pytest.mark.parametrize("table", config.TABLES)
def test_event_schema_is_valid_json_schema(table):
    schema = event_schema(table)
    jsonschema.Draft202012Validator.check_schema(schema)
    assert set(META_COLUMNS).issubset(schema["properties"])
    assert schema["properties"]["before"]["type"] == ["object", "null"]
    assert schema["properties"]["after"]["type"] == ["object", "null"]
    assert schema["required"] == list(META_COLUMNS)


def test_after_image_carries_the_primary_key(tmp_path):
    schema = event_schema("orders")
    assert "order_id" in schema["properties"]["after"]["properties"]


def test_a_valid_landed_record_passes_its_schema():
    schema = event_schema("tariffs")
    record = {
        "_table": "tariffs",
        "_op": "c",
        "_op_name": "create",
        "_row_key": "t-1",
        "_lsn": 42,
        "_event_ms": 1,
        "_event_ts": "2026-08-30T00:00:00+00:00",
        "_source_ts_ms": 1,
        "_ingested_ts": "2026-08-30T00:00:01+00:00",
        "_deleted": False,
        "before": None,
        "after": {"tariff_id": "t-1", "tariff_code": "X"},
    }
    jsonschema.validate(record, schema)
