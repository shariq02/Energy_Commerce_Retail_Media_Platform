# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT WEATHER MISSINGNESS RECONCILIATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`station_id`, `parameter_source_code`).
# MAGIC
# MAGIC **Sources:** `dwd_missingness_reconciliation` (Silver, energy_silver).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing to see where DWD's own
# MAGIC reported gaps (`dwd_missing_value_periods`) disagree with the nulls
# MAGIC actually observed in the 14 DWD hourly measurement tables -- was
# MAGIC previously a DQ table with no Gold consumer at all.
# MAGIC
# MAGIC **Purpose:** promote `dwd_missingness_reconciliation` to Gold, near
# MAGIC pass-through. `weather_station_key` is resolved against the *current*
# MAGIC `dim_weather_station` row only (`valid_to IS NULL`) -- this table has no
# MAGIC observation timestamp, so a point-in-time join (`pit_join()`) has nothing
# MAGIC to resolve against; a station retired before its current SCD row gets
# MAGIC NULL rather than an arbitrarily-picked historical row.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/04_fact_weather_missingness_reconciliation"
RID = gold_run_id()
GOLD_TABLE = "fact_weather_missingness_reconciliation"

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_missingness_reconciliation
_recon_silver = read_silver("dwd_missingness_reconciliation")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_weather_station (current rows only)
_station_current = read_gold("dim_weather_station", source="dwd").filter(
    F.col("valid_to").isNull()
)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the current weather_station_key
fact = resolve_fk(
    _recon_silver,
    _station_current,
    fact_key_cols=["station_id"],
    dim_key_cols=["station_id"],
    dim_surrogate_col="weather_station_key",
    output_col="weather_station_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn(
    "weather_missingness_reconciliation_key",
    surrogate_key("station_id", "parameter_source_code"),
)
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (station_id, parameter_source_code)
assert_unique_grain(
    fact,
    ["station_id", "parameter_source_code"],
    component=COMPONENT,
    source=SOURCE,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_weather_missingness_reconciliation
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_weather_missingness_reconciliation + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_source_code"],
    df_before=_recon_silver,
    extra_checks={
        "observed_only_count": fact.filter(
            F.col("_missingness_reconciliation_status") == "observed_only"
        ).count(),
        "reported_only_count": fact.filter(
            F.col("_missingness_reconciliation_status") == "reported_only"
        ).count(),
        "weather_station_key_unmatched_count": fact.filter(
            F.col("weather_station_key").isNull()
        ).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
