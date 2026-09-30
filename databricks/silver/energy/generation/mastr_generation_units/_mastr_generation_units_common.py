# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MASTR GENERATION UNITS SHARED HELPERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the two helpers shared by this folder's 6 carrier notebooks,
# MAGIC pulled in with `%run ./_mastr_generation_units_common` after
# MAGIC `_silver_common`. Definitions only -- no read/write inside (the bbox
# MAGIC helper writes to quarantine, same as `_silver_common.py`'s own
# MAGIC `write_quarantine`-calling helpers, never to Silver). Every caller still
# MAGIC does its own Read Bronze / Write Silver / Inspect in its own cells.
# MAGIC
# MAGIC References `NAME_MAP`, `SOURCE`, `RID` and `STRUCTURAL_CONSTANT_COLS`
# MAGIC from the caller's own Configuration cell (resolved at call time, not at
# MAGIC import time).

# COMMAND ----------

# DBTITLE 1,Helper -- drop structural constants (definition only)


def _drop_structural_constants(df):
    """Drop STRUCTURAL_CONSTANT_COLS by whichever name (raw or business-
    renamed) is actually present -- mastr_standardise may have already
    renamed them. Silently no-ops for a column that isn't present."""
    for raw in STRUCTURAL_CONSTANT_COLS:
        target = NAME_MAP.get(raw, raw)
        if target in df.columns:
            df = df.drop(target)
        elif raw in df.columns:
            df = df.drop(raw)
    return df


# COMMAND ----------

# DBTITLE 1,Helper -- quarantine out-of-Germany coordinates (definition only)


def quarantine_out_of_bbox(df, bronze_table: str) -> None:
    """Record (not drop) units whose coordinate falls outside the Germany
    bounding box -- `df` keeps every row."""
    bad = df.filter(bbox_outside_de("latitude", "longitude"))
    write_quarantine(
        _q_rows(
            bad,
            "coord_outside_de_bbox",
            "unit coordinate outside the Germany bounding box",
            "latitude,longitude",
            "latitude",
            "unit_id",
            SOURCE,
            bronze_table,
        ),
        RID,
    )
