# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM WEATHER STATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`station_id`, `valid_from`) -- time-varying (SCD).
# MAGIC Same-`valid_from` duplicates are canonicalised to the shortest `valid_to`;
# MAGIC `cap_scd_overlaps()` then caps a stale explicit `valid_to` that still runs
# MAGIC past a later, distinct `valid_from` (one-sided -- a genuine gap is left
# MAGIC alone). `dim_weather_station_name_history` is a separate small SCD at the
# MAGIC same grain -- its own windows aren't guaranteed to align with this one's.
# MAGIC
# MAGIC **Sources:** `dwd_station_geography`, `dwd_station_name_history` (Silver,
# MAGIC energy_silver_reference).
# MAGIC
# MAGIC **Serves use case:** point-in-time station resolution via `pit_join()`.
# MAGIC
# MAGIC **Purpose:** promote `dwd_station_geography` to Gold as the primary
# MAGIC time-varying station dimension `fact_weather` joins against.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/01_dim_weather_station"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_station_geography
_station_geography_silver = read_silver("dwd_station_geography")

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_station_name_history
_station_name_history_silver = read_silver("dwd_station_name_history")

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station (canonical row per key)
# Silver keeps every relocation-record variant -- Gold keeps exactly one
# canonical row per (station_id, valid_from), the one with the shortest
# valid_to, so pit_join() against fact_weather never fans out on a fact whose
# timestamp falls inside a shared, ambiguous window.
_w_station_dedup = Window.partitionBy("station_id", "valid_from").orderBy(
    F.asc_nulls_last("valid_to")
)
_station_geography_ranked = _station_geography_silver.withColumn(
    "_dedup_rank", F.row_number().over(_w_station_dedup)
)
_station_geography_canonical = _station_geography_ranked.filter(
    F.col("_dedup_rank") == 1
).drop("_dedup_rank")
_station_geography_alt_count = _station_geography_ranked.filter(
    F.col("_dedup_rank") > 1
).count()

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station (close + cap + verify SCD windows)
# close_scd_gaps() fills an open (NULL) valid_to; cap_scd_overlaps() caps a
# stale explicit one that still overruns the next valid_from.
# assert_no_overlapping_windows() hard-fails if any overlap remains.
_station_geography_canonical = close_scd_gaps(
    _station_geography_canonical, "station_id"
)
_station_geography_canonical = cap_scd_overlaps(
    _station_geography_canonical, "station_id"
)
_station_geography_capped_count = _station_geography_canonical.filter(
    F.col("_scd_overlap_capped")
).count()
assert_no_overlapping_windows(
    _station_geography_canonical,
    "station_id",
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station
dim_weather_station = _station_geography_canonical.drop(
    "_scd_overlap_capped"
).withColumn("weather_station_key", surrogate_key("station_id", "valid_from"))
dim_weather_station = add_gold_provenance(dim_weather_station, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_weather_station, one row per (station_id, valid_from)
assert_unique_grain(
    dim_weather_station,
    ["station_id", "valid_from"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_weather_station
write_gold(
    dim_weather_station,
    "dim_weather_station",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_weather_station + export findings
_findings_blocks = inspect_gold_table(
    dim_weather_station,
    "dim_weather_station",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from"],
    df_before=_station_geography_silver,
    extra_checks={
        "relocation_variant_rows_excluded": _station_geography_alt_count,
        "cross_row_scd_overlaps_capped": _station_geography_capped_count,
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_weather_station",
    "dim_weather_station",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station_name_history (canonical row per key)
# Same relocation-style overlap and same shortest-valid_to tie-break as
# dwd_station_geography above.
_w_name_history_dedup = Window.partitionBy("station_id", "valid_from").orderBy(
    F.asc_nulls_last("valid_to")
)
_station_name_history_ranked = _station_name_history_silver.withColumn(
    "_dedup_rank", F.row_number().over(_w_name_history_dedup)
)
_station_name_history_canonical = _station_name_history_ranked.filter(
    F.col("_dedup_rank") == 1
).drop("_dedup_rank")
_station_name_history_alt_count = _station_name_history_ranked.filter(
    F.col("_dedup_rank") > 1
).count()

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station_name_history (close + cap + verify SCD windows)
# Same open-ended-window and cross-row overlap risk as dwd_station_geography
# -- not currently pit_join()'d by anything, kept correct anyway (see
# _gold_common.py).
_station_name_history_canonical = close_scd_gaps(
    _station_name_history_canonical, "station_id"
)
_station_name_history_canonical = cap_scd_overlaps(
    _station_name_history_canonical, "station_id"
)
_station_name_history_capped_count = _station_name_history_canonical.filter(
    F.col("_scd_overlap_capped")
).count()
assert_no_overlapping_windows(
    _station_name_history_canonical,
    "station_id",
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_station_name_history
dim_weather_station_name_history = _station_name_history_canonical.drop(
    "_scd_overlap_capped"
).withColumn(
    "weather_station_name_history_key", surrogate_key("station_id", "valid_from")
)
dim_weather_station_name_history = add_gold_provenance(
    dim_weather_station_name_history, SOURCE, RID
)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_weather_station_name_history
assert_unique_grain(
    dim_weather_station_name_history,
    ["station_id", "valid_from"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_weather_station_name_history
write_gold(
    dim_weather_station_name_history,
    "dim_weather_station_name_history",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_weather_station_name_history + export findings
_findings_blocks = inspect_gold_table(
    dim_weather_station_name_history,
    "dim_weather_station_name_history",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "valid_from"],
    df_before=_station_name_history_silver,
    extra_checks={
        "relocation_variant_rows_excluded": _station_name_history_alt_count,
        "cross_row_scd_overlaps_capped": _station_name_history_capped_count,
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dim_weather_station_name_history",
    "dim_weather_station_name_history",
    _findings_blocks,
)
