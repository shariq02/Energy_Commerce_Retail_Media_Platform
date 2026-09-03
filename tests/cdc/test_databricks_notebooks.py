"""CDC -- static checks on the Databricks CDC notebooks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

The notebooks run on Databricks, not in pytest -- these checks guard the parts
that must stay in step with the rest of the pipeline: the catalog name, all 10
tables, and the two-representation design.
"""

from __future__ import annotations

import ast

import pytest

from src.ingestion.cdc import config

pytestmark = [pytest.mark.cdc, pytest.mark.unit, pytest.mark.databricks]

_NB_DIR = config.REPO_ROOT / "databricks"
_SETUP = _NB_DIR / "setup" / "01_create_cdc_objects.py"
_LOAD = _NB_DIR / "streaming" / "01_cdc_bronze_load.py"
_RECON = _NB_DIR / "streaming" / "02_cdc_reconciliation.py"


@pytest.mark.parametrize("path", [_SETUP, _LOAD, _RECON])
def test_notebook_parses_and_names_the_catalog(path):
    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert 'CATALOG = "energy_commerce_retail_media"' in text
    for table in config.TABLES:
        assert table in text


def test_setup_creates_history_and_current():
    text = _SETUP.read_text(encoding="utf-8")
    assert "cdc_{table}_history" in text
    assert "cdc_{table}_current" in text
    assert "cdc_operational_landing" in text


def test_load_appends_history_and_merges_current():
    text = _LOAD.read_text(encoding="utf-8")
    assert "MERGE INTO" in text
    assert "_history" in text and "_current" in text
    assert "s._lsn > t._lsn" in text  # LSN-guarded merge
    assert "_deleted = true" in text  # delete handling


def test_reconciliation_compares_against_postgres_snapshot():
    text = _RECON.read_text(encoding="utf-8")
    assert "post_change_row_counts.json" in text
    assert "quality.quality_audit_log" in text
    assert "_deleted = false" in text
