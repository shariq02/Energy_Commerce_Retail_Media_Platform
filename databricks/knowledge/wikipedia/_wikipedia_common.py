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
# MAGIC **Purpose:** constants, run id, provenance columns and the audit-log gate
# MAGIC shared by the Wikipedia notebooks. Pulled in with `%run ./_wikipedia_common`
# MAGIC after `%run ./_wikipedia_text`. Same shape as `databricks/gold/_gold_common.py`.
# MAGIC Definitions only -- no side effects at import.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt
import os
from pathlib import Path

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Constants
SOURCE_ROOT = "/Volumes/samples/databricks/datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet"
CATALOG = "energy_commerce_retail_media"
SCHEMA = "knowledge"
QUALITY_SCHEMA = "quality"
STAGE = "knowledge"
AUDIT_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.quality_audit_log"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_article"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_selected"
CLICKSTREAM_ROOT = "/Volumes/samples/databricks/datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json"
CLICKSTREAM_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_clickstream"
EDGES_TABLE = f"{CATALOG}.{SCHEMA}.wikipedia_edges"
LINK_TYPE = "link"
SNAPSHOT = "wikipedia_samples_2015"
SOURCE_SYSTEM = "wikipedia_samples"
FINDINGS_SOURCE = "wikipedia"
TERMS_RELATIVE = ("ai", "knowledge_corpus", "wikipedia", "selection_terms.yml")

# COMMAND ----------

# DBTITLE 1,Repository helpers


def repo_root():
    path = os.path.abspath(os.getcwd())
    while not os.path.isdir(os.path.join(path, "ai", "knowledge_corpus")):
        if os.path.dirname(path) == path:
            raise RuntimeError("repository root not found from the notebook folder")
        path = os.path.dirname(path)
    return path


def load_selection_terms():
    return load_terms(Path(repo_root(), *TERMS_RELATIVE))


# COMMAND ----------

# DBTITLE 1,Run id and clock


def run_id():
    return _dt.datetime.now(_dt.UTC).strftime("knowledge-%Y%m%dT%H%M%SZ")


def now_utc():
    return _dt.datetime.now(_dt.UTC)


# COMMAND ----------

# DBTITLE 1,Provenance columns


def add_knowledge_provenance(df, source_dataset, rid):
    return (
        df.withColumn("source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("source_dataset", F.lit(source_dataset))
        .withColumn("_knowledge_loaded_at", F.current_timestamp())
        .withColumn("_knowledge_run_id", F.lit(rid))
    )


# COMMAND ----------

# DBTITLE 1,Hard-fail gate with audit log


def check(
    component, source, metric_name, condition, *, detail="", metric_value=None, rid
):
    # Logs to quality.quality_audit_log (stage 'knowledge') and raises when the
    # condition is False, as the Gold check() does.
    status = "PASS" if condition else "FAIL"
    row = spark.createDataFrame(
        [
            (
                rid,
                _dt.datetime.now(_dt.UTC).date(),
                source,
                STAGE,
                component,
                metric_name,
                None if metric_value is None else float(metric_value),
                None,
                status,
                detail or None,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, run_date date, source string, stage string, component string, "
        "metric_name string, metric_value double, threshold double, status string, "
        "error_detail string, recorded_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    if not condition:
        raise RuntimeError(
            f"KNOWLEDGE GATE FAILED: {component}.{metric_name} -- {detail or 'no detail'}"
        )
