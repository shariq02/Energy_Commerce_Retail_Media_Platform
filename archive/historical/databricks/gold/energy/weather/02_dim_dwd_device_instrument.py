# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM DWD DEVICE INSTRUMENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`station_id`, `parameter_category`, `valid_from`)
# MAGIC -- time-varying (SCD2), distinct from `dim_weather_station`. Per-instrument
# MAGIC placement/height (e.g. wind sensor vs. temperature sensor) genuinely
# MAGIC varies independently of the station's own nominal location.
# MAGIC
# MAGIC **Sources:** `dwd_device_instrument` (Silver, energy_silver_reference) --
# MAGIC same overlap-repair steps as `dim_weather_station` (canonicalise, close
# MAGIC gaps, cap cross-row overlap), partitioned on (`station_id`,
# MAGIC `parameter_category`) together.
# MAGIC
# MAGIC **Serves use case:** per-instrument point-in-time resolution via `pit_join()`.
# MAGIC
# MAGIC **Purpose:** promote `dwd_device_instrument` to a governed, overlap-safe
# MAGIC Gold dimension.

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
COMPONENT = "gold/energy/weather/02_dim_dwd_device_instrument"
RID = gold_run_id()
GOLD_TABLE = "dim_dwd_device_instrument"

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_device_instrument
_device_instrument_silver = read_silver("dwd_device_instrument")

# COMMAND ----------

# DBTITLE 1,Transform -- dim_dwd_device_instrument (partition key for the SCD repair)
# The SCD helpers take one column -- a composite key stands in for
# (station_id, parameter_category) and is dropped before write.
_di = _device_instrument_silver.withColumn(
    "_scd_key", F.concat_ws("||", "station_id", "parameter_category")
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_dwd_device_instrument (canonical row per key)
_w_di_dedup = Window.partitionBy("_scd_key", "valid_from").orderBy(
    F.asc_nulls_last("valid_to")
)
_di_ranked = _di.withColumn("_dedup_rank", F.row_number().over(_w_di_dedup))
_di_canonical = _di_ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
_di_alt_count = _di_ranked.filter(F.col("_dedup_rank") > 1).count()

# COMMAND ----------

# DBTITLE 1,Transform -- dim_dwd_device_instrument (close + cap + verify SCD windows)
_di_canonical = close_scd_gaps(_di_canonical, "_scd_key")
_di_canonical = cap_scd_overlaps(_di_canonical, "_scd_key")
_di_capped_count = _di_canonical.filter(F.col("_scd_overlap_capped")).count()
assert_no_overlapping_windows(
    _di_canonical,
    "_scd_key",
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_dwd_device_instrument
dim_dwd_device_instrument = _di_canonical.drop(
    "_scd_key", "_scd_overlap_capped"
).withColumn(
    "dwd_device_instrument_key",
    surrogate_key("station_id", "parameter_category", "valid_from"),
)
dim_dwd_device_instrument = add_gold_provenance(dim_dwd_device_instrument, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (station_id, parameter_category, valid_from)
assert_unique_grain(
    dim_dwd_device_instrument,
    ["station_id", "parameter_category", "valid_from"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- dim_dwd_device_instrument
write_gold(
    dim_dwd_device_instrument, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_dwd_device_instrument + export findings
_findings_blocks = inspect_gold_table(
    dim_dwd_device_instrument,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_category", "valid_from"],
    df_before=_device_instrument_silver,
    extra_checks={
        "same_key_variant_rows_excluded": _di_alt_count,
        "cross_row_scd_overlaps_capped": _di_capped_count,
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
