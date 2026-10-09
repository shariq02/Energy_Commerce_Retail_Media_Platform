# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE -- WIKIPEDIA SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** constants, table names, Volume folders and small helpers shared
# MAGIC by the Wikipedia preparation notebooks. Pulled in with
# MAGIC `%run ./_wikipedia_common`. Definitions only -- no side effects at import.
# MAGIC The local command `scripts/knowledge/wikipedia_fetch.py` reads the Gold
# MAGIC table and the report table.

# COMMAND ----------

# DBTITLE 1,Imports
import json
import os
import sys
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Constants
SOURCE_ROOT = "/Volumes/samples/databricks/datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet"
CATALOG = "energy_commerce_retail_media"
SCHEMA = "knowledge"
VOLUME = "wikipedia_corpus"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_article"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_selected"
REPORT_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_build_report"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
REPORT_DIR = f"{VOLUME_ROOT}/reports"
SNAPSHOT = "wikipedia_samples_2015"
TERMS_RELATIVE = ("ai", "knowledge_corpus", "wikipedia", "selection_terms.yml")

# COMMAND ----------

# DBTITLE 1,Repository helpers


def repo_root():
    path = os.path.abspath(os.getcwd())
    while not os.path.isdir(os.path.join(path, "scripts", "knowledge")):
        if os.path.dirname(path) == path:
            raise RuntimeError("repository root not found from the notebook folder")
        path = os.path.dirname(path)
    return path


def load_preparation():
    # The preparation library of scripts/knowledge, imported from the Git folder.
    root = repo_root()
    if root not in sys.path:
        sys.path.insert(0, root)
    from scripts.knowledge import wikipedia_prepare

    return wikipedia_prepare


def load_selection_terms(wp):
    return wp.load_terms(Path(repo_root(), *TERMS_RELATIVE))


# COMMAND ----------

# DBTITLE 1,Report helpers


def write_report(folder, name, data):
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, name), "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)


def read_report(folder, name):
    with open(os.path.join(folder, name), encoding="utf-8") as handle:
        return json.load(handle)
