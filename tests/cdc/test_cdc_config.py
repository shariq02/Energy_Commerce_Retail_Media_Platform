"""CDC configuration -- topic strategy and shared constants.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import pytest

from src.ingestion.cdc import config

pytestmark = [pytest.mark.cdc, pytest.mark.unit]

_EXPECTED_TABLES = {
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "orders",
    "order_items",
}


def test_all_seven_physical_tables_present():
    assert set(config.TABLES) == _EXPECTED_TABLES
    assert len(config.TABLES) == 7
    assert "order_items" in config.TABLES


def test_one_topic_per_table_unique():
    topics = config.all_topics()
    assert len(topics) == 7
    assert len(set(topics)) == 7
    for table in config.TABLES:
        assert config.topic_for(table) == f"ecrmap.operational.{table}"


def test_every_table_has_a_primary_key():
    assert set(config.PRIMARY_KEY) == _EXPECTED_TABLES
    assert config.PRIMARY_KEY["order_items"] == "order_item_id"


def test_config_module_matches_cdc_module():
    from config import DEBEZIUM_CONFIG, KAFKA_TOPICS

    assert set(DEBEZIUM_CONFIG["tables"]) == _EXPECTED_TABLES
    assert DEBEZIUM_CONFIG["snapshot_mode"] == "initial"
    assert DEBEZIUM_CONFIG["plugin_name"] == "pgoutput"
    assert set(KAFKA_TOPICS.values()) == set(config.all_topics())
