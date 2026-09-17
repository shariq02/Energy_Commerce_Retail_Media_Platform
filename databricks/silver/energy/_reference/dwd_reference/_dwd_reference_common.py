# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # DWD REFERENCE SHARED HELPERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the three helpers shared by this folder's DWD reference/
# MAGIC metadata notebooks, pulled in with `%run ./_dwd_reference_common` after
# MAGIC `_silver_common`. Definitions only -- no read/write inside, and no
# MAGIC side effects at import. Every caller still does its own Read Bronze /
# MAGIC Write Silver / Inspect in its own cells; these helpers are pure
# MAGIC DataFrame-in, DataFrame-out transform steps, same category as
# MAGIC `_silver_common.py`'s own `mastr_standardise` / `attach_city_ags`.
# MAGIC
# MAGIC References `SOURCE`, `RID` and `STATION_IDS` from the caller's own
# MAGIC Configuration cell (resolved at call time, not at import time -- the
# MAGIC same non-strict-binding pattern `_silver_common.py` uses for `spark`).

# COMMAND ----------

# DBTITLE 1,Helper -- normalise a raw station-id column (definition only)


def clean_station_id(id_col):
    return F.regexp_replace(F.trim(F.col(id_col)), r"\.0$", "")


# COMMAND ----------

# DBTITLE 1,Helper -- quarantine trailer/header rows (definition only)


def keep_real_stations(df, id_col, bronze_table):
    """Row-quarantine parser-trailer / spurious-header rows (id not a curated
    numeric station id). The kept frame has `station_id` normalised."""
    df = df.withColumn("station_id", clean_station_id(id_col))
    kept, q = row_quarantine(
        df,
        ~F.col("station_id").isin(STATION_IDS),
        rule_id="metadata_trailer_row",
        reason="id column not a curated numeric station id (trailer/header row)",
        field_name=id_col,
        value_col="station_id",
        sr_id_col="station_id",
        source_system=SOURCE,
        bronze_table=bronze_table,
    )
    write_quarantine(q, RID)
    return kept.drop(id_col)


# COMMAND ----------

# DBTITLE 1,Helper -- rename metadata columns to business names (definition only)


def rename_meta(df, bronze_table):
    for col in TABLES[bronze_table]["columns"]:
        n = col["name"]
        tgt = META_FIELDS.get(n)
        if tgt and tgt != n and n in df.columns:
            df = df.withColumnRenamed(n, tgt)
    return df