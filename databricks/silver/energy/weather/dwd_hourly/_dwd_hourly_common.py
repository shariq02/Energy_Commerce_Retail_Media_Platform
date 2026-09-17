# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # DWD HOURLY MEASUREMENT SHARED HELPER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the one transform recipe shared by all 14 DWD hourly-
# MAGIC measurement notebooks in this folder -- identical across every
# MAGIC parameter, purely data-driven by the contract's own per-table column
# MAGIC list. Pulled in with `%run ./_dwd_hourly_common` after `_silver_common`.
# MAGIC Definition only -- no read/write of the primary table inside (the
# MAGIC quarantine write is a side table, same category as `resolve_conflicts`'s
# MAGIC own callers elsewhere). Every caller still does its own Read Bronze /
# MAGIC Write Silver / Inspect in its own cells.
# MAGIC
# MAGIC References `SOURCE`, `RID`, `TABLES`, `CODED` and `NAME_MAP` from the
# MAGIC caller's own Configuration cell (resolved at call time, not at import
# MAGIC time).

# COMMAND ----------

# DBTITLE 1,Helper -- the DWD hourly measurement transform recipe (definition only)


def transform_dwd_measurement(bronze_df, table_name: str):
    """Bronze -> Silver for one DWD hourly measurement table: strip sentinels,
    resolve same-key conflicts (quarantining the losers), cast to logical
    types, decode QN + any coded columns, apply business-name renames, build
    the provenance key, parse MESS_DATUM to observation_ts (UTC), attach city
    AGS, add provenance. Identical recipe for all 14 tables."""
    cols = [c["name"] for c in TABLES[table_name]["columns"]]
    qn_col = next(c for c in cols if c.startswith("QN_"))
    value_cols = [
        c
        for c in cols
        if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor")
        and not c.startswith("QN_")
    ]

    df = strip_sentinels(bronze_df, [*value_cols, qn_col])

    df, q = resolve_conflicts(
        df,
        ["STATIONS_ID", "MESS_DATUM"],
        value_cols,
        qn_col=qn_col,
        bronze_table=table_name,
    )
    q = q.withColumn("source_system", F.lit(SOURCE))
    write_quarantine(q, RID)

    df = cast_logical(df, TABLES[table_name]["columns"])
    df = decode_qn(df, qn_col)

    for raw, en_map in CODED.items():
        if raw in df.columns:
            pref = NAME_MAP.get(raw, raw)
            df = decode_via_labeled_map(df, raw, pref, en_map).drop(raw)

    df = apply_renames(df, NAME_MAP)
    df = df.withColumn("_srid", sha_key(F.lit(table_name), "STATIONS_ID", "MESS_DATUM"))
    df = df.withColumn("observation_ts", parse_mess_datum("MESS_DATUM")).drop(
        "MESS_DATUM"
    )
    df = attach_city_ags(df, "city")
    df = add_provenance(df, SOURCE, "_srid", RID)
    return df
