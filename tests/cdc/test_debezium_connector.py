"""CDC -- the Debezium connector and worker property files, and the publication.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import pytest

from src.ingestion.cdc import config

pytestmark = [pytest.mark.cdc, pytest.mark.unit]

_CONNECTOR = config.REPO_ROOT / "cdc" / "debezium" / "ecrmap-postgres.properties"
_WORKER = config.REPO_ROOT / "cdc" / "config" / "connect-standalone.properties"
_PUBLICATION = config.REPO_ROOT / "postgres" / "migrations" / "0002_cdc_publication.sql"


def _props(path):
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip()
    return out


@pytest.fixture(scope="module")
def connector():
    return _props(_CONNECTOR)


def test_connector_core_settings(connector):
    assert connector["connector.class"].endswith("PostgresConnector")
    assert connector["plugin.name"] == "pgoutput"
    assert connector["snapshot.mode"] == "initial"
    assert connector["publication.autocreate.mode"] == "disabled"
    assert connector["slot.name"] == "ecrmap_cdc_slot"
    assert connector["publication.name"] == "ecrmap_cdc_pub"
    assert connector["topic.prefix"] == "ecrmap"
    assert connector["tombstones.on.delete"] == "true"
    assert connector["decimal.handling.mode"] == "string"


def test_connector_captures_all_ten_tables(connector):
    listed = {t.strip() for t in connector["table.include.list"].split(",")}
    expected = {f"operational.{t}" for t in config.TABLES}
    assert listed == expected
    assert "operational.order_items" in listed


def test_worker_uses_json_converter_and_env_provider():
    worker = _props(_WORKER)
    assert worker["value.converter"].endswith("JsonConverter")
    assert worker["value.converter.schemas.enable"] == "true"
    assert worker["config.providers"] == "env"
    assert "cdc/plugins" in worker["plugin.path"]


def test_publication_migration_lists_all_ten_tables():
    sql = _PUBLICATION.read_text(encoding="utf-8").lower()
    assert "create publication ecrmap_cdc_pub" in sql
    for table in config.TABLES:
        assert f"operational.{table}" in sql
    assert "0002_cdc_publication" in sql
